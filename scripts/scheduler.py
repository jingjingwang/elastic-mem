#!/usr/bin/env python

import jvm
import myria_utils
import subprocess
import sys
import os
import time
import copy
import math
import random
import gendata
import csv
import json
import threading
import weka.core.serialization as serialization
from weka.classifiers import Classifier
from weka.core.dataset import Instance
import weka.core.jvm as wekajvm
from collections import deque
from threading import Thread
from get_instances import filter_instance
import qplan
import ingest_tpch
from gendata import gen
from train import parse_gclog, dimen

TB = 10 * 1000
EPS = 0.001
M = 1048576
K = 1024
HISTORY_LEN = 3
interval = 0.1
SAFE = 10 * M
TOOLONG = 60*8
MEMUNIT = 4 * M

WORKER_JVM_BASE = 6014 
MASTER_REST_BASE = 7014
MASTER_RPC_BASE = 8014
WORKER_RPC_BASE = 9014
WORKER_REST_BASE = 5014

classifiers = {}
history = {}
resubmit = deque()
running = {}
result = {}
past = []
deploys = []
killtime = []
runninglock = threading.Lock()
running_queries = {}
foutname = ''
initrss = 0
initemax = 0
initomax = 0
init_shrinkrss_count = init_shrinkrss_size = 0
allqtime = 0.0
allgctime = 0.0
alltoolong = 0
qtime = 0.0
fqtime = 0.0
allfinished = 0
resubmitted = []

def get_queries(numq, mode):
  queryset = []
  if mode == 'random':
    queries = [
    ('join','I_L_L_L_S52'),
    ('join','I_I_I_L_L_S20'),
    ('join','I_L_L_L_S4'),
    ('agg','I_I_L_L_L_S10_S10'),
    ('agg','I_I_I_I_I_S2_S2'),
    ('joinagg','I_I_I_L_S36'),
    ('aggjoin','I_I_I_L_S26_S26'),
    ('aggjoin','I_I_I_S34_S34')]
    s = []  
    for j in range(numq):
      q = {'ops':queries[j][0], 'nT':400*TB, 'nK':400*TB, 'nT_delta':400*TB, 'nK_delta':400*TB, 'schema':queries[j][1], 'par':[1], 'qindex':j}
      s.append(q)
    queryset.append(s)
  elif mode == 'randgen':
    queries = []
    for intc in [1,2,3,4,5]:
      for longc in [0,1,2,3,4]:
        for strc in [0,1,2]:
          for strsum in [0] if strc == 0 else [4,20,36,52,68]:
            if strc == 0 or strsum == 0: strlen = 0
            else: strlen = strsum / strc
            for q in ['join','agg','joinagg','aggjoin']:
              if q == 'aggjoin' and longc == dimen('long', 'max'): continue
              types = qplan.genschema(intc, longc, strc, strlen)
              queries.append({'ops':q, 'nT':400*TB, 'nK':400*TB, 'nT_delta':400*TB, 'nK_delta':400*TB, 'schema':types, 'par':[1]})
    for i in range(1):
      s = [] 
      for j in range(numq):
        q = queries[random.randint(0, len(queries)-1)]
        q['qindex'] = j
        s.append(q)
      queryset.append(s)
  elif mode == 'fixed':
    types = 'I_I_I_I_I_L_L_L_L_S68_S68'
    combinations = [
      ['agg','join','joinagg','aggjoin'] #,['agg','agg','agg','agg'],['join','join','join','join']#,['agg','join','agg','join']
      #['agg','agg','agg','agg'] #, #,
      #['join', 'join', 'join', 'join'] #,'agg'] #,'join','join']
    ]
    for ops in combinations:
      #for par in [[1],[2,1],[1,1]]:
      for par in [[1]]: #,[2,1]]:
        s = []
        for i in range(numq):
          s.append({'ops':ops[i], 'nT':400*TB, 'nK':400*TB, 'nT_delta':400*TB, 'nK_delta':400*TB, 'schema':types, 'par':par, 'qindex':len(s)})
        queryset.append(s)
  return queryset

def get_tpch(numq, mode):
  queryset = []
  for qids in [['4.1','9.1','18.1','19.1','4.2','9.2','18.2','19.2']]:
    s = []
    for par in [[1]]: #,[2,1]]: TODO: how to control skew?
      for i in range(len(qids)):
        s.append({'par':par,'tpch':qids[i]})
    queryset.append(s)
  return queryset

def emptycost():
  return {'OOM':0, 'cost':0.0, 'NOOP':0, 'NOGC':0}

def load_models():
  global classifiers
  for op in ['hash']:
    for obj in ['yreal', 'oreal', 'ylsize', 'olsize']: #, 'ydsize', 'odsize']:
      model = op + ',' + obj
      if os.path.isfile("models/%s" % model):
        classifiers[model] = Classifier(jobject=serialization.read("models/%s" % model))

def add_features(line):
  nTPre = float(line['nT']) - float(line['nT_delta'])
  line['nTdeltaLong'] = float(line['nT_delta']) * float(line['long'])
  line['nTdeltaStr'] = float(line['nT_delta']) * float(line['str'])
  line['nTdeltaStrSum'] = float(line['nT_delta']) * float(line['strsum'])
  line['nTdeltaLogStrSum'] = float(line['nT_delta']) * math.log(float(line['strsum'])+1)
  line['nTLong'] = float(line['nT']) * float(line['long'])
  line['nTStr'] = float(line['nT']) * float(line['str'])
  line['nTStrSum'] = float(line['nT']) * float(line['strsum'])
  line['nTLogStrSum'] = float(line['nT']) * math.log(float(line['strsum'])+1)
  line['nTPreLong'] = nTPre * float(line['long'])
  line['nTPreStr'] = nTPre * float(line['str'])
  line['nTPreStrSum'] = nTPre * float(line['strsum'])
  line['nTPreLogStrSum'] = nTPre * math.log(float(line['strsum'])+1)

def addf():
  return [
  'nTdeltaLong', 'nTdeltaStr', 'nTdeltaLogStrSum', 'nTdeltaStrSum',
  'nTLong', 'nTStr', 'nTStrSum', 'nTLogStrSum',
  'nTPreLong', 'nTPreStr', 'nTPreStrSum', 'nTPreLogStrSum'
]

def add_delta_and_schema(opstats, lastgcopstats):
  for op in opstats:
    keys = list(opstats[op].keys())
    for k in ['nT','nK']:
      if op in lastgcopstats and k in lastgcopstats[op]:
        opstats[op][k + '_delta'] = opstats[op][k] - lastgcopstats[op][k]
      else:
        opstats[op][k + '_delta'] = opstats[op][k]
    add_features(opstats[op])

def predict(obj, opstats, tpch=True):
  threshold = {'ylsize':1, 'ydsize':1, 'olsize':1, 'odsize':1, 'yreal':0.01, 'oreal':0.01}
  s = 0.0
  for op in opstats:
    if len(opstats[op]) <= 1: continue
    values = [opstats[op][k] for k in ['long', 'nK', 'nK_delta', 'nT', 'nT_delta', 'str', 'strsum']]
    values.append(0) # should be obj
    for k in addf():
      values.append(opstats[op][k])
    v = classifiers['hash,' + obj].classify_instance(Instance.create_instance(values))
    #print obj, op, values, v
    s += v
  #else:
  #  zeroref = {'nT':1,'nT_delta':0,'nK':1,'nK_delta':0,'long':1,'str':0,'strsum':0}
  #  s = manual_pred(obj, zeroref)
  #  for op in opstats:
  #    prediction = manual_pred(obj, opstats[op])
  #    s = s + prediction - manual_pred(obj, zeroref)
  return max(s, threshold[obj])

def predict_heap_future(history):
  max_eden_used_delta = 10 * M
  for i in range(HISTORY_LEN):
    idx = len(history)-i-1
    if idx < 0: break
    if 'eused' not in history[idx]: continue
    pre = 0 if 'eused' not in history[idx-1] else history[idx-1]['eused']
    max_eden_used_delta = max(max_eden_used_delta, history[idx]['eused'] - pre)
  return history[-1]['eused'] + max_eden_used_delta

def merge_cost(cost, decision):
  newcost = copy.deepcopy(cost)
  if decision['action'] == 'OOM': newcost['OOM'] += 1
  elif decision['action'] == 'NOOP': newcost['NOOP'] += 1
  elif decision['action'] in ['FGC_NOPM', 'FGC_PM', 'YGC', 'NOGC']:
    newcost['cost'] += decision['cost']
  return newcost

def better(newcost, cost):
  if cost is None: return True
  if newcost is None: return False
  if newcost['OOM'] != cost['OOM']: return newcost['OOM'] < cost['OOM']
  if newcost['NOOP'] != cost['NOOP']: return newcost['NOOP'] < cost['NOOP']
  return newcost['cost'] < cost['cost']

def ceiling(a, memincre):
  aa = int(int(a) / memincre)
  if aa * memincre < a: aa += 1
  return aa

def ceilings(a, b, memincre):
  return (ceiling(a, memincre), ceiling(b, memincre))

def transfer(opt, idx, m, mq, decision, heap, memincre, i):
  mq = ceilings(mq[0], mq[1], memincre)
  if mq[0] * memincre > initemax: return
  if mq[1] * memincre > initomax: return
  if m < mq[0] + mq[1]: return
  if decision['action'] in ['YGC','FGC_PM','FGC_NOPM']:
    #decision['time'] += mmap_time(decision['space'])
    decision['cost'] = decision['time'] / decision['space']
  elif decision['action'] in ['NOGC']:
    decision['space'] = mq[0] * memincre - history[i][-1]['ecap'] + mq[1] * memincre - history[i][-1]['ocap']
    if decision['space'] > 0:
      decision['time'] = mmap_time(decision['space'])
      decision['cost'] = decision['time'] / decision['space']
    else:
      decision['cost'] = 0
  if idx == 0: newcost = merge_cost(emptycost(), decision)
  elif opt[idx-1][m - mq[0] - mq[1]] is None: return
  else: newcost = merge_cost(opt[idx-1][m - mq[0] - mq[1]][0], decision)
  if opt[idx][m] is None or better(newcost, opt[idx][m][0]):
    opt[idx][m] = [newcost, mq, decision['action']]

def pick_kill(queries, scheme, jvms, conf):
  for i in jvms:
    if i not in scheme: scheme[i] = ['NOOP', history[i][-1]['ecap'], history[i][-1]['ocap']]
  maxm, maxq = -1, None
  cnt_time = time.time()
  global running_queries, resubmit, allqtime
  count = 0
  for q in range(len(queries)):
    if q not in running_queries or running_queries[q]['qid'] is None: continue
    summ = 0
    for w in deploys[q]['workers']:
      if w not in jvms:
        print w, jvms, 'not in'
        summ = -1
        break
      summ += history[w][-1]['eused'] + history[w][-1]['oused']
    if summ == -1: continue
    if conf['pickkill'] == 'mem':
      if maxm < summ: maxm, maxq = summ, q
    elif conf['pickkill'] == 'memtime':
      t = cnt_time - running_queries[q]['submit_time']
      if maxm < summ / t: maxm, maxq = summ / t, q
  for w in deploys[maxq]['workers']:
    if w in scheme: scheme[w] = ['OOM', 0, 0]
  allqtime += cnt_time - running_queries[maxq]['submit_time']
  running_queries[maxq]['qid'] = None

def append_resub(conf, resubq, running_before):
  global resubmit, resubmitted
  if not conf['resubmit']: return
  print 'append resub', running_before, resubq, resubmitted
  if resubq in resubmitted: return
  resubmit.append(resubq)
  resubmitted.append(resubq)

def check_resubmit(conf):
  global running, resubmit
  if not conf['resubmit']: return
  print 'check resubmit', resubmit, len(running)
  if len(resubmit) == 0: return
  if len(running) >= 1: return
  q = resubmit.popleft()
  if q is None: return
  myria_utils.launch('localhost', deploys[q])
  time.sleep(0.5)
  myria_utils.pre_submit_plan('localhost', deploys[q])
  submit_thread(0, pick_plan(conf['queries'][q]),
                conf['queries'][q]['schema'] if 'schema' in conf['queries'][q] else 'tpch' + str(conf['queries'][q]['tpch']),
                None if conf['runningqlen'] is None else conf['runningqlen']*len(conf['queries'][q]['par']), q).start()
  hashtable_stats_thread(q, deploys[q]['workers'][0]).start()
  
def check_allnoop(queries, jvms, scheme):
  runq = 0
  progressq = 0 
  for idx, q in enumerate(queries):
    run = True
    for w in deploys[idx]['workers']:
      if w not in jvms or w not in scheme: run = False
    if run:
      runq += 1
      noprogress = 0
      for w in deploys[idx]['workers']:
        if history[w][-1]['gcactive']:
          continue
        #if w in scheme and scheme[w][0] in ['NOOP']: 
        #  if history[w][-1]['ecap'] == 0:
        #    noprogress = len(deploys[idx]['workers'])
        #    break
        if scheme[w][0] in ['NOOP']:
          noprogress = len(deploys[idx]['workers'])
          break
        if scheme[w][0] in ['NOGC'] and len(history[w]) >= 2 and 'action' in history[w][-2] and history[w][-2]['action'] == scheme[w][0]:
          if history[w][-1]['ecap'] == 0:
            noprogress = len(deploys[idx]['workers'])
            break
          if history[w][-2]['eused'] == history[w][-1]['eused']:
            noprogress += 1
      if noprogress != len(deploys[idx]['workers']): progressq += 1 # this query makes progress
  #print progressq, runq
  return progressq < runq / 2 or progressq == 0


def allocate(conf, jvms, elapsed):
  global history
  tpch = ('tpch' in conf['queries'][0])
  for idx, i in enumerate(jvms):
    s = history[i][-1] 
    s['yreal'], s['oreal'] = predict('yreal', s['opstats'], tpch), predict('oreal', s['opstats'], tpch)
    s['ylsize'], s['olsize'] = predict('ylsize', s['opstats'], tpch) * K, predict('olsize', s['opstats'], tpch) * K
    if s['eused'] < s['ylsize']: s['ylsize'], s['ydsize'] = s['eused'], 0
    else: s['ydsize'] = s['eused'] - s['ylsize']
    if s['oused'] < s['olsize']: s['olsize'], s['odsize'] = s['oused'], 0
    else: s['odsize'] = s['oused'] - s['olsize']
    s['next_yused'] = predict_heap_future(history[i])

  best = {'cost':None, 'scheme':{}, 'memincre':MEMUNIT}
  for i in reversed(range(len(jvms)+1)):
    enum_opt(jvms, i, 0, [], conf, best)
    if len(best['scheme']) == len(jvms): break # return with the smallest # of NOOPs
  scheme = best['scheme']
  #print scheme
  if check_allnoop(conf['queries'], jvms, scheme):
    print 'allnoop', scheme
    pick_kill(conf['queries'], scheme, jvms, conf)

  for i in scheme:
    s = history[i][-1]
    s['action'], s['ynewcap'], s['onewcap'] = scheme[i][0], scheme[i][1], scheme[i][2]
  adjust(scheme, conf, elapsed, best['memincre']) 

def enum_opt(jvms, numset, cnt, cntset, conf, best):
  if len(cntset) == numset:
    scheme = {}
    for i in jvms:
      if history[i][-1]['gcactive']:
        scheme[i] = ['INGC', history[i][-2]['ynewcap'], history[i][-2]['onewcap']]
      elif i not in cntset:
        scheme[i] = ['NOOP', history[i][-1]['ecap'], history[i][-1]['ocap']]
    totalmem = conf['totalmem']
    for i in jvms:
      if history[i][-1]['gcactive'] or i not in cntset:
        totalmem -= history[i][-2]['ynewcap'] + history[i][-2]['onewcap']
    if conf['memincre'].startswith('fix'):
      memincre = int(conf['memincre'][4:]) * M 
    elif conf['memincre'].startswith('dyn'):
      sumfree = totalmem
      for i in cntset:
        sumfree -= history[i][-1]['eused'] + history[i][-1]['oused']
      memincre = int(sumfree / int(conf['memincre'][4:]))
      memincre = max(ceiling(memincre, MEMUNIT) * MEMUNIT, MEMUNIT)
    while totalmem / memincre < 2 * len(cntset) and memincre/2 >= MEMUNIT: memincre /= 2
    cost, setscheme = dp(cntset, memincre, totalmem, conf)
    scheme.update(setscheme)
    if better(cost, best['cost']):
      best['cost'], best['scheme'], best['memincre'] = cost, scheme, memincre
    return
  for i in range(cnt, len(jvms)):
    if history[jvms[i]][-1]['gcactive']: continue
    cntset.append(jvms[i])
    enum_opt(jvms, numset, i+1, cntset, conf, best)
    del cntset[-1]

def noprogress(seq):
  i = len(seq)-2
  while i >= 0:
    if 'action' in seq[i]:
      if seq[i]['action'] in ['FGC_PM', 'FGC_NOPM']: return True
      if seq[i]['action'] not in ['INGC', 'NOOP']: return False
    i -= 1
  assert False

def dp(jvms, memincre, totalmem, conf):
  if len(jvms) == 0: return None, {}
  STEPS = totalmem / memincre
  opt = [[None for i in range(STEPS+1)] for j in range(len(jvms))]
  for idx, i in enumerate(jvms):
    s = history[i][-1] 
    ygc_save, fgc_save = s['ydsize'], s['ydsize'] + s['odsize']
    ygc_time, fgc_nopm_time = s['yreal'], s['oreal'] + s['yreal']
    s['olsize'] = max(s['olsize'], 1)
    fgc_pm_time = s['oreal'] * (s['ylsize'] + s['olsize'] + 0.0) / (s['olsize']) + s['yreal'] # 0
    o_gc_nopm_cap, o_gc_pm_cap = (s['ylsize'] + s['oused']) * 1.0, s['oused']
    y_nogc_cap, y_gc_cap = s['next_yused'], s['eused']
    pre = history[i][-2]
    if 'action' in pre:
      if i in gchistory and gchistory[i][-1]['action'] in ['FGC_PM', 'FGC_NOPM'] and noprogress(history[i]): # need check why necessary
        fgc_save = min(fgc_save, s['lastgcsaved'])
        ygc_save = min(ygc_save, s['lastgcsaved'])
        #if fgc_save > conf['gcsavecap'] or ygc_save > conf['gcsavecap']:
        #  print 'shrinking gc save', 'lastgcsaved', s['lastgcsaved'], fgc_save, ygc_save, y_gc_cap, o_gc_nopm_cap, o_gc_pm_cap, memincre, i
      elif pre['action'] == 'NOGC' and pre['blocked_count'] < s['blocked_count']:
        #print 'expanding blocked', pre['blocked_count'], s['blocked_count'], y_nogc_cap, pre['ynewcap'], memincre, i
        y_nogc_cap = max(y_nogc_cap, pre['ynewcap'] + memincre)
    if 'pmfailure' in s and s['pmfailure'] > 0 and i in gchistory: o_gc_nopm_cap = max(o_gc_nopm_cap, gchistory[i][-1]['onewcap'] + memincre)
    for m in range(STEPS + 1):
      transfer(opt, idx, m, (y_nogc_cap, o_gc_nopm_cap * 1.1), {'action':'NOGC'}, s, memincre, i) 
      if fgc_save > conf['gcsavecap']:
        transfer(opt, idx, m, (y_gc_cap, o_gc_nopm_cap), {'action':'FGC_NOPM', 'time':fgc_nopm_time, 'space':fgc_save}, s, memincre, i)
        transfer(opt, idx, m, (y_gc_cap, o_gc_pm_cap), {'action':'FGC_PM', 'time':fgc_pm_time, 'space':fgc_save}, s, memincre, i)
      if ygc_save > conf['gcsavecap']:
        transfer(opt, idx, m, (y_gc_cap, o_gc_nopm_cap), {'action':'YGC', 'time':ygc_time, 'space':ygc_save}, s, memincre, i)
  bestm = -1
  for m in range(STEPS + 1):
    if opt[len(jvms)-1][m] is not None and (bestm == -1 or better(opt[len(jvms)-1][m][0], opt[len(jvms)-1][bestm][0])): bestm = m
  if bestm == -1: return None, {}
  bestcost = opt[len(jvms)-1][bestm][0]
  scheme = {}
  for idx in reversed(range(len(jvms))):
    scheme[jvms[idx]] = [opt[idx][bestm][2], opt[idx][bestm][1][0] * memincre, opt[idx][bestm][1][1] * memincre]
    bestm -= opt[idx][bestm][1][0] + opt[idx][bestm][1][1]
  return bestcost, scheme

def mmap_time(size):
  if size <= 0: return 0
  return 0.35 * size / (1024 * M)

def select_conf(q):
  return {"source":{"dataType":"File","filename":gendata.genfilename(q)}, "schema":qplan.short_to_schema(q['schema'])}

def getstrlen(types):
  for t in types.split('_'):
    if t.startswith('S'): return int(t[1:])
  return 0

def pick_plan(q):
  with open('/home/ec2-user/tpch/q%s.json' % q['tpch']) as data_file:
    return json.load(data_file)

def post_queries(conf, deployment):
  p = int(subprocess.check_output("df | grep xvda1 | awk \"{print \\$5}\"", shell=True).strip()[:-1])
  if p >= 80: subprocess.call("rm -f /data/*_*", shell=True)

  myria_utils.kill_java_python('localhost')
  global initrss, initemax, initomax
  initrss, initemax, initomax = myria_utils.get_rss_gen_max('localhost', deploys[0])
  print 'initrss', initrss, 'emax omax', initemax, initomax
  global init_shrinkrss_count, init_shrinkrss_size
  init_shrinkrss_count, init_shrinkrss_size = myria_utils.get_shrink_rss_before_query('localhost', deploys[0]['workers'][0][4])
  print 'init shrinkrss', init_shrinkrss_count, init_shrinkrss_size
  global init_expand_count, init_expand_size
  init_expand_count, init_expand_size = myria_utils.get_expand_before_query('localhost', deploys[0]['workers'][0][4])
  print 'init expand', init_expand_count, init_expand_size
  myria_utils.drop_cache('localhost')
  time.sleep(1)
  myria_utils.kill_java_python('localhost')
  time.sleep(0.5)
  for idx, q in enumerate(conf['queries']):
    print 'launching', idx
    myria_utils.launch('localhost', deploys[idx])
    myria_utils.pre_submit_plan('localhost', deploys[idx])

  for idx, q in enumerate(conf['queries']):
    for w in deploys[idx]['workers']:
      ycap, ocap = jvm.get('localhost', w[4], ['ecap', 'ocap']).strip().split('|')[:2]
      history[w] = deque()
      history[w].append({'ynewcap':int(ycap), 'onewcap':int(ocap)})
  for idx, q in enumerate(conf['queries']):
    submit_thread(conf['queries'][idx]['delay'], pick_plan(conf['queries'][idx]),
                  conf['queries'][idx]['schema'] if 'schema' in conf['queries'][idx] else 'tpch' + str(conf['queries'][idx]['tpch']),
                  None if conf['runningqlen'] is None else conf['runningqlen']*len(conf['queries'][idx]['par']), idx).start()
    hashtable_stats_thread(idx, deploys[idx]['workers'][0]).start()

class hashtable_stats_thread(Thread):
  def __init__(self, qindex, worker):
    Thread.__init__(self)
    self.qindex = qindex
    self.worker = worker
  def run(self):
    worker = self.worker
    while True:
      if worker in running and 'qid' in running[worker]:
        qid = running[worker]['qid']
        break
      time.sleep(0.1)
    while True:
      r = myria_utils.hash_table_stats('localhost', self.worker[3], qid)
      if r == 'timeout':
        pass
      elif r is None: break
      elif worker in running:
        running[worker]['opstats_latest'] = r

def parse_opstats(s):
  ret = {}
  for opstat in s.strip().split('\t'):
    if len(opstat.strip()) == 0: continue
    t = opstat.strip().split(' ')
    if len(t) == 0: continue
    if t[0] not in ret: ret[t[0]] = {}
    for idx, m in enumerate(['nT', 'nK', 'long', 'str', 'strsum']):
      ret[t[0]][m] = int(t[idx+1])
  return ret

def query_finished(r):
  if r is None: return 2
  try:
    s = r["status"]
  except requests.exceptions.RequestException as e:
    return 0
  if s == "SUCCESS":
    return 1
  if s == "ERROR" or s == "KILLED" or s == "UNKNOWN":
    return 2
  return -1

def query_elapsed(r):
  try:
    return float(r["elapsedNanos"]) / 1000000000.
  except TypeError:
    return 0.0

def check_running(conf, myjava, timestamp = None):
  global running
  global allqtime, fqtime, allfinished, alltoolong
  running_before = len(running)
  for idx in range(len(conf['queries'])):
    if idx not in running_queries or running_queries[idx] is None: continue
    qid = running_queries[idx]['qid']
    if qid is None: continue
    status = myria_utils.query_status('localhost', deploys[idx]['master_rest'], qid)
    r = query_finished(status)
    elapsed = query_elapsed(status)
    if r >= 0:
      print 'finished', idx, qid, 'ok' if r == 1 else 'error'
      for t in deploys[idx]['workers']:
        if t not in running: continue
        collect_log_thread(idx, t, copy.deepcopy(running[t]), 'ok' if r == 1 else 'error').start()
        del running[t]
      running_queries[idx]['qid'] = None
      if r == 1:
        allfinished += 1
        fqtime += elapsed
      check_resubmit(conf)
      allqtime += elapsed
      continue
    if elapsed > TOOLONG: 
      print 'toolong', idx, qid
      for t in deploys[idx]['workers']:
        if t not in running: continue
        collect_log_thread(idx, t, copy.deepcopy(running[t]), 'toolong').start()
        del running[t]
      running_queries[idx]['qid'] = None
      append_resub(conf, idx, running_before)
      check_resubmit(conf)
      allqtime += elapsed
      alltoolong += 1
  ready = {}
  sumrss = 0
  global initrss
  for idx, i in enumerate(list(copy.deepcopy(running.keys()))):
    running[i]['myjava'] = myjava
    if myjava:
      paras = ['intriggeredgc', 'eused', 'oused', 'blocked_count', 'ecap', 'ocap', 'pmfailure', 'lastgcsaved', 'cpu', 'blocked_sleep', 'rss', 'shrink_rss_count', 'shrink_rss_size', 'expand_count', 'expand_size']
      ret = jvm.get('localhost', i[4], paras).strip().split('|')
      if not (len(ret) == len(paras)+1 and len(ret[3]) > 0): continue
      t = {}
      for idx, k in enumerate(paras):
        t[k] = ret[idx]
      for k in ['eused', 'oused', 'blocked_count', 'ecap', 'ocap', 'pmfailure', 'lastgcsaved', 'rss', 'shrink_rss_count', 'shrink_rss_size', 'expand_count', 'expand_size']:
        running[i][k] = int(t[k])
      for k in ['cpu', 'blocked_sleep']:
        running[i][k] = float(t[k])
      running[i]['timestamp'], running[i]['gcactive'] = timestamp, t['intriggeredgc'] == '1'
      running[i]['opstats'] = parse_opstats(running[i]['opstats_latest'] if 'opstats_latest' in running[i] else '')
      if running[i]['pmfailure'] == 0 and 'opstats_beforegc' in running[i] and len(running[i]['opstats_beforegc']) > 0:
        running[i]['opstats_nopmgc'] = running[i]['opstats_beforegc']
        running[i]['opstats_beforegc'] = ''
      add_delta_and_schema(running[i]['opstats'], running[i]['opstats_nopmgc'] if 'opstats_nopmgc' in running[i] else {})
      for op in running[i]['opstats']:
        qplan.more_schema_feature(running[i]['opstats'][op])
      ready[i] = copy.deepcopy(running[i])
    else:
      ret = jvm.get('localhost', i[4], ['cpu', 'rss']).strip().split('|')
      if len(ret) == 3: running[i]['cpu'], running[i]['rss'] = float(ret[0]), int(ret[1])
      else: running[i]['rss'] = 0 #
      running[i]['blocked_count'], running[i]['blocked_sleep'] = 0, 0.0
      running[i]['shrink_rss_count'], running[i]['shrink_rss_size'] = 0, 0
      running[i]['expand_count'], running[i]['expand_size'] = 0, 0
      ready[i] = copy.deepcopy(running[i])
    sumrss += ready[i]['rss'] - initrss
  conf['maxrss'] = max(conf['maxrss'], sumrss)
  return ready

class gendata_thread(Thread):
  def __init__(self, qindex, dep, q):
    Thread.__init__(self)
    self.qindex = qindex
    self.dep = dep
    self.q = q
  def run(self):
    if 'tpch' in self.q:
      pass
    else: gen(self.q)

class submit_thread(Thread):
  def __init__(self, delay, plan, types, runningqlen, qindex):
    Thread.__init__(self)
    self.d = delay
    self.p = plan
    self.t = types
    self.l = runningqlen
    self.qindex = qindex
  def run(self):
    time.sleep(self.d)
    global running
    global running_queries
    global runninglock
    print 'submit thread', self.qindex, self.t, self.l, len(running), deploys[self.qindex], self.d
    if self.l is not None:
      while True:
        runninglock.acquire()
        if len(running) + len(deploys[self.qindex]['workers']) <= self.l:
          qid = myria_utils.submit_plan(self.p, 'localhost', deploys[self.qindex]['master_rest'])
          for i in deploys[self.qindex]['workers']:
            running[i], result[i] = {'qid':qid, 'types':self.t}, {}
          print 'current running len', len(running), self.l
          runninglock.release()
          break
        runninglock.release()
        time.sleep(random.random()*0.2)
    else:
      qid = myria_utils.submit_plan(self.p, 'localhost', deploys[self.qindex]['master_rest'])
      for i in deploys[self.qindex]['workers']:
        running[i], result[i] = {'qid':qid, 'types':self.t}, {}
        running[i]['working_dir'] = myria_utils.working_dir('localhost', i[3])
    if self.qindex not in running_queries: running_queries[self.qindex] = {}
    running_queries[self.qindex]['qid'] = qid
    if 'submit_time' not in running_queries[self.qindex]: running_queries[self.qindex]['submit_time'] = time.time()

class collect_log_thread(Thread):
  def __init__(self, qindex, instance, s, status):
    Thread.__init__(self)
    self.i = instance
    self.s = s
    self.status = status
    self.qindex = qindex
  def run(self):
    global deploys
    s = self.s
    rest = deploys[self.qindex]['master_rest']
    elapsed = myria_utils.query_elapsed('localhost', rest, s['qid'])
    myria_utils.kill_keyword('localhost', 'JVMPort=%d' % deploys[self.qindex]['workers'][0][4])
    gclog_file = 'logs/%d_%d_%s_%d_gclog' % (self.i[4], self.qindex, s['qid'], self.i[0])
    subprocess.call("cp %s/gclog %s" % (s['working_dir'], gclog_file), shell=True)
    subprocess.call("cp %s/evaluator.stdout logs/%s_%d_%s_%d_stdout" % (s['working_dir'], self.i[4], self.qindex, s['qid'], self.i[0]), shell=True)
    subprocess.call("cp %s/evaluator.stderr logs/%s_%d_%s_%d_stderr" % (s['working_dir'], self.i[4], self.qindex, s['qid'], self.i[0]), shell=True)
    gcs = parse_gclog(gclog_file, None, s['myjava'])[1:]
    ss = 0.0
    for gc in gcs: 
      if 'yreal' in gc: ss += gc['yreal']
      if 'oreal' in gc: ss += gc['oreal']
    print gclog_file, ss
    global allgctime
    allgctime += ss
    result[self.i].update({'gcs':gcs, 'querytime':elapsed, 'status':self.status, 'qid':s['qid'], 'cpu':s['cpu'], 'blocked_count':s['blocked_count'], 'blocked_sleep':s['blocked_sleep'], 'shrink_rss_count':s['shrink_rss_count'], 'shrink_rss_size':s['shrink_rss_size'], 'expand_count':s['expand_count'], 'expand_size':s['expand_size']})

def adjust(scheme, conf, elapsed, memincre):
  running_before = len(running)
  global gchistory
  global killtime
  for i in scheme:
    print i, scheme[i]
    decision = scheme[i][0]
    if conf['debug']: # and decision in ['FGC_NOPM','FGC_PM','YGC']:
      s = history[i][-1]
      print '* state transfering for ', i, s['qid'], s['timestamp']
      print '    ', 'opstats', s['opstats']
      print '    ', 'eden used cap', s['eused'], s['ecap'], 'old used cap', s['oused'], s['ocap'], 'blocked', s['blocked_count'], s['blocked_sleep'], 'gcactive', s['gcactive'], 'cpu', s['cpu'], 'lastgcsaved', s['lastgcsaved']
      print '    ', 'predict', 'yreal', s['yreal'], 'ylsize', s['ylsize'], 'ydsize', s['ydsize'], 'oreal', s['oreal'], 'olsize', s['olsize'], 'odsize', s['odsize'], 'next_yused', s['next_yused']
      print '    ', i, scheme[i]
      if len(history[i]) >= 2 and 'next_yused' in history[i][-2]:
        print 'grow', i, s['yused'], history[i][-2]['next_yused']
    if decision == 'OOM': 
      for vm in deploys[i[1]]['workers']:
        if vm in running:
          collect_log_thread(i[1], vm, running[vm], 'kill').start()
          print 'kill', vm
          del running[vm]
      append_resub(conf, i[1], running_before)
      check_resubmit(conf)
      killtime.append(str(elapsed))
      continue
    if decision in ['NOOP', 'INGC']: continue
    args = ['emax=%d' % scheme[i][1], 'omax=%d' % scheme[i][2]]
    if decision in ['FGC_NOPM','FGC_PM','YGC']:
      if i not in gchistory: gchistory[i] = []
      gchistory[i].append(history[i][-1])
      if decision.startswith('FGC'): decision += '_' + str(memincre)
      args.append(decision.lower())
      running[i]['opstats_beforegc'] = parse_opstats(running[i]['opstats_latest'] if 'opstats_latest' in running[i] else '')
    jvm.send('localhost', i[4], args)

def gen_deploy(dep, queries, cap):
  global deploys
  jvm_options = ['-XX:ParallelGCThreads=1', '-XX:+PrintGCDetails', '-XX:+PrintGCTimeStamps', '-XX:-UseTLAB', '-XX:+UseParallelGC', '-XX:-UseParallelOldGC', '-XX:MarkSweepAlwaysCompactCount=1', '-XX:-UseAdaptiveSizePolicy', '-ElasticMem', '-XX:GCTriggerMode=1', '-XX:+ScavengeAfterFullGC', '-Xloggc:gclog']
  for i in range(len(queries)):
    deploys.append({'name':'%s.%d' % (dep, i), 'master_rest':MASTER_REST_BASE+i, 'master_rpc':MASTER_RPC_BASE+i, 'workers':[(0, i, WORKER_RPC_BASE+i, WORKER_REST_BASE+i, WORKER_JVM_BASE+i)]})
    with open('/data/myria/myriadeploy/%s.%d' % (dep, i), 'w') as fout:
      fout.write('[deployment]\n')
      fout.write('path = /data/myria/q%d\n' % i)
      fout.write('dbms = sqlite\n')
      fout.write('name = q%d\n' % i)
      fout.write('rest_port = %d\n' % deploys[i]['master_rest'])
      fout.write('[master]\n')
      fout.write('0 = localhost:%d\n' % deploys[i]['master_rpc'])
      fout.write('[workers]\n')
      fout.write('1 = localhost:%d:::%d:%d\n' % (deploys[i]['workers'][0][2], deploys[i]['workers'][0][3], deploys[i]['workers'][0][4]))
      fout.write('[persist]\n')
      fout.write('persist_uri = hdfs://vega.cs.washington.edu:8020\n')
      fout.write('[runtime]\n')
      fout.write('jvm.options = %s\n' % ' '.join(jvm_options))
      fout.write('container.master.vcores.number = 1\n')
      fout.write('container.worker.vcores.number = 1\n')
      fout.write('jvm.master.heap.size.min.gb = 0.9\n')
      fout.write('jvm.master.heap.size.max.gb = 0.9\n')
      fout.write('jvm.worker.heap.size.min.gb = 0.9\n')
      fout.write('jvm.worker.heap.size.max.gb = %d\n' % cap)
      fout.write('container.master.memory.size.gb = 0.9\n')
      fout.write('container.worker.memory.size.gb = %d\n' % (cap+1))
  return deploys

def ingest_data(conf, deploy):
  ingest_tpch.download('/data')

def run_my_scheduler(conf):
  prev_count = threading.active_count()

  ingest_data(conf, gen_deploy('deployment.cfg.myjava', conf['queries'], 250))
  post_queries(conf, gen_deploy('deployment.cfg.myjava', conf['queries'], 500))

  global history
  global running
  global resubmit
  begin_time = pre_time = time.time()
  idx = roundtime = roundcount = 0
  while True:
    time.sleep(conf['roundsleep'])
    cnt_time = time.time()
    current_running = check_running(conf, True, idx)
    if len(current_running) == 0 and len(resubmit) == 0 and threading.active_count() == prev_count: break
    for i in current_running:
      if i not in history: history[i] = deque()
      #if len(history[i]) >= HISTORY_LEN: history[i].popleft()
      history[i].append(current_running[i])
    if len(current_running) > 0: allocate(conf, sorted(list(current_running.keys())), cnt_time - begin_time)
    idx += 1
  print_result(conf, roundtime / roundcount if roundcount > 0 else None, time.time() - begin_time)

def run_original_jvm(conf):
  ingest_data(conf, gen_deploy('deployment.cfg.myjava', conf['queries'], 250))
  cap = conf['totalmem'] * 1.0 / M / conf['runningqlen'] / len(conf['queries'][0]['par'])
  post_queries(conf, gen_deploy('deployment.cfg.ori', conf['queries'], cap))
  global running
  begin_time = time.time()
  while True:
    time.sleep(conf['roundsleep'])
    current_running = check_running(conf, False)
    if len(current_running) == 0 and threading.active_count() == 2: break
  print_result(conf, None, time.time() - begin_time)

def print_result(conf, avground, elapsed):
  global init_shrinkrss_count, init_shrinkrss_size
  allcpu = 0.0
  allblocked_count = allblocked_sleep = allshrink_count = allshrink_size = allexpand_count = allexpand_size = 0
  for i in result:
    while 'qid' not in result[i]: time.sleep(1)
    #print 'result', i, result[i]['qid']
    #sumgctime = 0
    #for idx, gc in enumerate(result[i]['gcs']):
      #print idx, gc
      #if 'yreal' in gc: sumgctime += gc['yreal']
      #if 'oreal' in gc: sumgctime += gc['oreal']
      #if conf['which'] == 'ori': continue
      #print '  ', gchistory[i][idx]['action'],
      #if 'yreal' in gc:
      #  print 'yreal', gc['yreal'], gchistory[i][idx]['yreal'], 'ylsize', gc['ylsize'], gchistory[i][idx]['ylsize'] / K, 'ydsize', gc['ydsize'], gchistory[i][idx]['ydsize'] / K,
      #if 'oreal' in gc:
      #  print 'oreal', gc['oreal'], gchistory[i][idx]['oreal'], 'olsize', gc['olsize'], gchistory[i][idx]['olsize'] / K, 'odsize', gc['odsize'], gchistory[i][idx]['odsize'] / K,
      #print 'esued', gchistory[i][idx]['eused'] / K
    #print 'qid', result[i]['qid'], 'querytime', result[i]['querytime'], 'cpu', result[i]['cpu'], 'gctime', sumgctime, 'status', result[i]['status'], 'blocked', result[i]['blocked_count'], result[i]['blocked_sleep'], 'shrink', result[i]['shrink_rss_count'], result[i]['shrink_rss_size'], 'expand', result[i]['expand_count'], result[i]['expand_size']
    #allgctime += sumgctime
    allcpu += result[i]['cpu']
    allblocked_count += result[i]['blocked_count']
    allblocked_sleep += result[i]['blocked_sleep']
    allshrink_count += result[i]['shrink_rss_count'] - init_shrinkrss_count
    allshrink_size += result[i]['shrink_rss_size'] - init_shrinkrss_size
    allexpand_count += result[i]['expand_count'] - init_expand_count
    allexpand_size += result[i]['expand_size'] - init_expand_size
  #print 'allqtime', allqtime, 'fqtime', fqtime, 'allgctime',  allgctime, 'allcpu', allcpu, 'allfinished', allfinished, 'allblocked_count', allblocked_count, 'allblocked_sleep',allblocked_sleep, 'elapsed',elapsed, 'allshrink_count', allshrink_count, 'allshrink_size', allshrink_size, 'allexpand_count', allexpand_count, 'allexpand_size', allexpand_size, 'alltoolong', alltoolong
  header = sorted(list(copy.deepcopy(conf.keys())) + ['allqtime', 'allgctime', 'allcpu', 'allfinished', 'allblocked_count', 'allblocked_sleep', 'avground', 'killtime', 'delay', 'elapsed', 'fqtime', 'allshrink_count', 'allshrink_size', 'allexpand_count', 'allexpand_size', 'alltoolong'])
  if not os.path.isfile(foutname) or os.path.getsize(foutname) < 10:
    with open(foutname, 'w') as f:
      writer = csv.DictWriter(f, fieldnames=header)
      writer.writeheader()
  with open(foutname, 'a') as f:
    writer = csv.DictWriter(f, fieldnames=header)
    line = copy.deepcopy(conf)
    line.update({'allqtime':allqtime, 'fqtime':fqtime, 'allgctime':allgctime, 'allcpu':allcpu, 'allfinished':allfinished, 'allblocked_count':allblocked_count, 'allblocked_sleep':allblocked_sleep, 'avground':avground, 'killtime':'_'.join(killtime), 'delay':'_'.join([str(q['delay']) for q in line['queries']]), 'elapsed':elapsed, 'allshrink_count':allshrink_count, 'allshrink_size':allshrink_size, 'allexpand_count':allexpand_count, 'allexpand_size':allexpand_size, 'alltoolong':alltoolong})
    line['queries'] = shrinkqset(line['queries'])
    for k in line:
      if isinstance(line[k], float): line[k] = '%.2f' % line[k] 
    writer.writerow(line)

def doit(conf):
  global past
  for line in past:
    found = True
    for k in line:
      if k == 'queries' and shrinkqset(conf[k]) != line[k] or k != 'queries' and conf[k] != line[k]:
        found = False
        break
    if found:
      return
  print '@@@@', conf
  global history, running, gchistory, result, killtime, running_queries, allfinished, allqtime, fqtime, alltoolong, allgctime, resubmitted
  history = {}
  running = {}
  running_queries = {} 
  gchistory = {}
  result = {}
  killtime = []
  resubmitted = []
  allfinished = alltoolong = 0
  allqtime = 0.0
  allgctime = 0.0
  fqtime = 0.0
  if conf['which'] == 'ori': run_original_jvm(conf)
  else: run_my_scheduler(conf)
  subprocess.call('scp %s jwang@dragon.cs.washington.edu:/home/jwang/project/elastic-mem' % foutname, shell=True)

def shrinkqset(qset):
  ret = []
  for q in qset:
    if 'schema' in q:
      types = ''.join([t for t in q['schema'] if t != '_'])
      ret.append(q['ops']+'_'+types+'_'+''.join([str(p) for p in q['par']]))
    else:
      ret.append('tpch_'+str(q['tpch'])+'_'+''.join([str(p) for p in q['par']]))
  return '-'.join(ret)

def load_past():
  if not os.path.isfile(foutname): return
  global past
  with open(foutname) as f:
    reader = csv.DictReader(f)
    for line in reader:
      p = {}
      for k in ['roundsleep']: p[k] = float(line[k])
      for k in ['delayrange','totalmem','runningqlen','gcsavecap']: p[k] = int(line[k]) if len(line[k]) > 0 else None
      for k in ['which','queries','memincre']: p[k] = line[k] if len(line[k]) > 0 else None
      past.append(p)

def main(argv):
  global foutname
  foutname = 'scheduler.%s.csv' % argv[1]
  queries = get_tpch(8, 'fixed')
  wekajvm.start()
  load_models()
  for qs in queries:
    for delayrange in [0]: #, 30, 60]: #, 90]:
      predelay = 0
      for idx, q in enumerate(qs):
        q['delay'] = predelay
        predelay += delayrange
      for mem in [80000]:
        for t in range(1):
          for which in ['my']:
            conf = {'totalmem':int(mem) * M, 'queries':qs, 'which':which, 'debug':False, 'delayrange': delayrange, 'maxrss':0}
            if len(argv) > 1 and argv[1] == 'debug': conf['debug'] = True
            if which == 'my':
              for pick in ['mem']: #,'memtime']:
                for resubmit in [False]:
                  for savecap in [30]:
                    for memincre in ['dyn_12']:
                      if resubmit and memincre != 'dyn_12': continue
                      for roundsleep in [0.5]: #0.1, 0.5, 1]:
                        conf['memincre'], conf['gcsavecap'], conf['roundsleep'], conf['maxrss'] = memincre, savecap * M, roundsleep, 0
                        conf['pickkill'], conf['resubmit'], conf['runningqlen'] = pick, resubmit, None
                        doit(conf)
            elif which == 'ori':
              conf['memincre'] = conf['gcsavecap'] = conf['pickkill'] = None
              conf['resubmit'] = False
              conf['roundsleep'] = 0.5
              numjvms = [8]
              #numjvms = [1, len(qs)/2, len(qs)] if delayrange > 0 else [len(qs)]
              for runningqlen in list(set(numjvms)):
                conf['runningqlen'] = runningqlen
                doit(conf)
  wekajvm.stop()

if __name__ == "__main__":
  main(sys.argv)
