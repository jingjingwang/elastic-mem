#!/usr/bin/env python

import json
import csv
import time
import random
import os.path
import subprocess
import requests
import re
import math
import sys
import threading
from sets import Set
from collections import deque
from threading import Thread
from collections import deque
from thread import start_new_thread
import myria_utils
import qplan
import gendata
import jvm
import copy
from get_instances import filter_instance
import ingest_tpch

working_instance = {}
jobs = deque()
exprs = []
dimens = ['long', 'nK', 'nK_delta', 'nT', 'nT_delta', 'str', 'strsum']
GRID_OUT = 'grid_out.csv'
GRID_POINTS = 'grid_points.csv'
RAND_OUT = 'rand_out.csv'
RAND_POINTS = 'rand_points.csv'
KEYNAME = "jwang.pem"
ERR_OUTPUT = 'err.csv'
QTIME_HASH = 'qtime_hash.csv'
QTIME_TPCH = 'qtime_tpch.csv'
WORKER_REST = 6015
MASTER_REST = 8964
WORKER_JVM = 7015
deploy = {'name':'deployment.cfg.train', 'master_rest':MASTER_REST}

def pick_one_instance(tag=None):
  while True:
    try:
      lines = filter_instance({'key':'tag-value', 'value':tag, 'return':'dns'})
    except:
      print sys.exc_info()[0]
      continue
    for ins in copy.deepcopy(working_instance.keys()):
      if working_instance[ins] == 'done':
        del working_instance[ins]
        print 'finished one, current', len(working_instance)
    for live in lines:
      if live not in working_instance:
        working_instance[live] = "before"
        print 'assigning instance', live, len(working_instance), len(lines)
        return live
    time.sleep(0.2)

def collect_data(conf, i, qid, success=True):
  paths = subprocess.check_output('ssh -i %s ec2-user@%s ps aux | grep REEF_LOCAL_RUNTIME | grep JVMPort | grep -v grep' % (KEYNAME, i), shell=True).split(' ')[-4].split(':')
  for p in paths:
    if 'REEF_LOCAL_RUNTIME' in p:
      workingdir = p[:p.index('/driver')]
      break
  paths = subprocess.check_output('ssh -i %s ec2-user@%s ls %s' % (KEYNAME, i, workingdir), shell=True).split()
  for p in paths:
    if 'Node-8' in p:
      workingdir += '/' + p
      break
  print 'collect data', i, qid, workingdir
  for f in ['gclog', 'evaluator.stdout', 'evaluator.stderr']:
    args = ["scp", "-i", KEYNAME, "-q", "ec2-user@%s:%s/%s" % (i, workingdir, f), "logs/%s_%s_%s" % (i, qid, f)]
    subprocess.call(args)
  p = int(subprocess.check_output("ssh -i %s ec2-user@%s df | grep data | awk \"{print \\$5}\"" % (KEYNAME, i), shell=True).strip()[:-1])
  if p >= 80: subprocess.call("ssh -i %s ec2-user@%s rm -f /data/*_*" % (KEYNAME, i), shell=True)
  if not success:
    with open(ERR_OUTPUT, 'a') as f:
      f.write(json.dumps(conf) + '\t' + 'error' + '\t' + i + '\t' + qid + '\n')
  gcs = parse_gclog('logs/%s_%s_gclog' % (i, qid), "logs/%s_%s_evaluator.stdout" % (i, qid))
  for gc in gcs:
    print gc
  print len(gcs)
  if conf['triggermode'] == 'training':
    if len(gcs) != 3:
      with open(ERR_OUTPUT, 'a') as f:
        f.write(json.dumps(conf) + '\t' + 'error' + '\t' + i + '\t' + qid + '\n')
      return None
    return gcs[2:]
  return gcs[1:]

def parse_gclog(gclog, stdout=None, socket_trigger=False):
  gcs = []
  preosize = 0
  r = {}
  with open(gclog) as f:
    for line in f.readlines():
      line = line.strip()
      if socket_trigger and line.startswith('===='):
        if len(r) > 0: gcs.append(r)
        r = {}
      if line.find("[GC") >= 0:
        if len(r) > 0 and 'yreal' in r:
          gcs.append(r)
          r = {}
        minor = myria_utils.parsegcline(line, 'y')
        if minor is None: break # should only happen for the last incomplete gc
        ylsize = minor["opostsize"] - minor["opresize"]
        ydsize = minor["ypresize"] - ylsize
        if ydsize < 0: ylsize, ydsize = minor["ypresize"], 0
        r.update({'ylsize':ylsize, 'ydsize':ydsize, 'ysys':minor['ysys'], 'yuser':minor['yuser'], 'yreal':minor['yreal']})
      elif line.find("[Full GC") >= 0:
        if len(r) > 0 and 'oreal' in r:
          gcs.append(r)
          r = {}
        major = myria_utils.parsegcline(line, 'o')
        if major is None: break # should only happen for the last gc
        odsize = major["opresize"] - major["opostsize"]
        olsize = preosize - odsize
        if olsize < 0: olsize, odsize = 0, preosize
        preosize = major["opostsize"]
        r.update({"olsize":olsize, "odsize":odsize, 'osys':major['osys'], 'ouser':major['ouser'], 'oreal':major['oreal']})
        r['order'] = 'before' if 'yreal' in r else 'after'
  if len(r) > 0: gcs.append(r)
  if stdout is None: return gcs
  gcs_with_op = []
  with open(stdout) as f:
    idx = 0
    for line in f.readlines():
      if line.startswith("hash_table_stats"):
        tokens = filter(None, line.strip().split('\t'))[1:]
        stats = {}
        for token in tokens:
          t = token.strip().split(' ')
          header = ['nT', 'nK', 'long', 'str', 'strsum']
          for i in range(len(header)):
            k = t[0] + '_' + header[i]
            stats[k] = int(t[i+1])
            if header[i] in ['nT','nK']:
              if idx > 0 and k in gcs[idx-1]: stats[k+"_delta"] = stats[k] - gcs[idx-1][k]
              else: stats[k+"_delta"] = stats[k]
        if idx >= len(gcs): break
        if len(tokens) == 0 and idx > 0: break
        gcs[idx].update(stats)
        gcs_with_op.append(copy.deepcopy(gcs[idx]))
        idx += 1
  return gcs_with_op

def submit_plan(plan, hostname):
  myria_utils.drop_cache(hostname)
  myria_utils.shutdown(hostname)
  time.sleep(1)
  myria_utils.launch(hostname, deploy)
  time.sleep(1)
  jvm.send(hostname, WORKER_JVM, ['emax=max', 'smax=0', 'omax=max', 'doneinit'])
  time.sleep(1)
  requests.get(myria_utils.url(hostname, WORKER_REST) + "sysgc")
  time.sleep(2)
  r = requests.post(myria_utils.url(hostname, MASTER_REST) + "query", data=json.dumps(plan), headers={'Content-Type':'application/json'})
  print r.text
  qid = str(r.json()["queryId"])
  print "running query %s plan %s on %s" % (qid, json.dumps(plan), hostname)
  return qid
  
class one_expr_thread(Thread):
  def __init__(self, args, ins, expr_id):
    Thread.__init__(self)
    self.s = args
    self.ins = ins
    self.expr_id = expr_id

  def get_hash_plan(self):
    s = self.s
    if s['str'] == 0 or s['strsum'] == 0: strlen = 0
    else: strlen = s['strsum'] / s['str'] 
    types = qplan.genschema(s['long'], s['str'], strlen) 
    s['schema'] = types
    filename = gendata.genfilename(s)
    select_conf = {'source':{'dataType':'File', 'filename':filename}, 'schema':qplan.short_to_schema(types)}
    if s['triggermode'] == 'training': trigger = [s['nT'] - s['nT_delta'], s['nT']]
    else: trigger = []
    subprocess.call('ssh -i jwang.pem ec2-user@%s ./gendata.py %d %d %d %d %s' % \
      (self.ins, s['nT'], s['nT_delta'], s['nK'], s['nK_delta'], s['schema']), shell=True)
    return qplan.one_hash_plan(select_conf, trigger)

  def get_tpch_plan(self, qid):
    myria_utils.shutdown(self.ins)
    myria_utils.launch(self.ins, deploy)
    time.sleep(1)
    jvm.send(self.ins, deploy['workers'][0][4], ['emax=max', 'smax=0', 'omax=max', 'doneinit'])
    time.sleep(1)
    ingest_tpch.ingesttpchdata(self.ins)
    with open('/home/jwang/project/tpch-dbgen/%s.json' % qid) as data_file:    
      return json.load(data_file)

  def run(self):
    s = self.s
    if self.expr_id is not None: return
    plan = self.get_tpch_plan(s['tpch_qid']) if 'tpch_qid' in s else self.get_hash_plan() 
    qid = submit_plan(plan, self.ins)
    time.sleep(0.5) # give it time to start before triggering gc
    result = None
    gcidx = 0
    while True:
      try:
        r = myria_utils.query_finished(self.ins, MASTER_REST, qid)
        if r >= 0:
          result = collect_data(self.s, self.ins, qid, r == 1)
          break
      except Exception as e:
        print e
        break
      if s['triggermode'] in ['training', 'no']: time.sleep(2)
      else:
        if isinstance(s['triggermode'], list):
          if gcidx < len(s['triggermode']):
            time.sleep(s['triggermode'][gcidx])
            requests.get(myria_utils.url(self.ins, WORKER_REST) + "sysgc")
            gcidx += 1
        else:
          while jvm.gcactive(self.ins, JVM_PORT): time.sleep(0.5)
          sleeptime = random.randint(0, int(s['triggermode']*0.9))
          print sleeptime, s['triggermode']
          time.sleep(sleeptime) #TODO: success == false
          requests.get(myria_utils.url(self.ins, WORKER_REST) + "sysgc")
          time.sleep(0.5) # make sure it's either short gc or long gc & in gacative
    working_instance[self.ins] = 'done'
    if s['triggermode'] == 'no': self.output_qtime(r == 1, qid, s['outf'])
    else: self.output_gcs(result, qid, s['outf'])

  def output_qtime(self, success, qid, outf):
    elapsed = myria_utils.query_elapsed(self.ins, MASTER_REST, qid)
    header = ['qtime','success','ins','qid']
    for k in ['long','str','strsum']:
      if k in self.s: header.append(k)
    if not os.path.isfile(outf) or os.path.getsize(outf) < 10:
      with open(outf, 'w') as f:
        f.write(','.join(header) + '\n')
    with open(outf, 'a') as f:
      r = {'qtime': elapsed, 'success':success, 'ins':self.ins, 'qid':qid}
      if 'qid' in self.s: r['qid'] = self.s['qid']
      for k in ['long','str','strsum']:
        if k in self.s: r[k] = self.s[k]
      line = []
      for h in header: line.append(str(r[h]))
      f.write(','.join(line) + '\n')
        
  def output_gcs(self, result, qid, outf):
    if result is None or len(result) == 0:
      print 'output gcs no result'
      return
    header = list(result[0].keys())
    header.extend(['gcidx', 'qid', 'ins', 'nT', 'nK', 'nT_delta', 'nK_delta']) #'%s_long' % op, '%s_strsum' % op, '%s_str' % op])
    header.sort()
    if not os.path.isfile(outf) or os.path.getsize(outf) < 10:
      with open(outf, 'w') as f:
        f.write(','.join(header) + '\n')
    with open(outf, 'a') as f:
      for idx, r in enumerate(result):
        r['gcidx'], r['qid'], r['ins'] = idx+1, qid, self.ins
        for k in ['nT', 'nK', 'nT_delta', 'nK_delta']:
          if k in self.s: r[k] = self.s[k]
          else: r[k] = ''
        for k in header:
          if k not in r:  # might be the last sysgc
            r = []
            break
        if len(r) == 0: continue
        line = []
        for h in header: line.append(str(r[h]))
        f.write(','.join(line) + '\n')

def valid(coor):
  for k in ['nT', 'nT_delta', 'nK', 'nK_delta', 'long', 'str', 'strsum']:
    if k not in coor: return 1
    if coor[k] > dimen(k, 'max'): return 2
    if coor[k] < dimen(k, 'min'): return 3
  if coor['str'] == 0 and coor['strsum'] > 0: return 4
  if coor['str'] > 0:
    strlen = coor['strsum'] / coor['str']
    if strlen < 1 and not change: return 5
    coor['strsum'] = coor['str'] * max(1, strlen)
    if coor['strsum'] > dimen('strsum', 'max'): return 6
  a = coor['nT_delta'] <= coor['nT'] and coor['nK_delta'] <= coor['nK'] and coor['nK'] <= coor['nT'] and \
      coor['nK_delta'] <= coor['nT_delta'] and coor['nK'] - coor['nK_delta'] <= coor['nT'] - coor['nT_delta']
  if not a: return 10
  if coor['nT'] > 0 and coor['nK'] == 0: return 11
  if coor['nT_delta'] > 0 and coor['nK_delta'] == 0: return 12
  if coor['nT'] - coor['nT_delta'] > 0 and coor['nK'] - coor['nK_delta'] == 0: return 13
  return 0

def did_expr(conf):
  for idx, expr in enumerate(exprs):
    same = True
    for k in dimens:
      if conf[k] != expr[k]:
        same = False
        break
    if same: return idx
  return None

def push_exprs(fin, fout):
  with open(fin) as f:
    reader = csv.DictReader(f)
    for idx, line in enumerate(reader):
      coor = {'triggermode': 'training', 'outf':fout}
      for k in line:
        coor[k] = int(line[k])
      jobs.append(coor)

def formkey(line):
  ret = []
  for k in ['long','str','strsum']:
    ret.append(str(line[k]))
  return '_'.join(ret)

def load_qtime():
  qtime = {}
  if os.path.isfile(QTIME_HASH):
    with open(QTIME_HASH) as f:
      reader = csv.DictReader(f)
      for line in reader:
        qtime[formkey(line)] = (line['qtime'], line['success'])
  return qtime

#qids = [1,2,3,4,5,6,8,9,10,11,12,14,15,16,17,18,19]
qids = [101,102,103,104,105,106,108,109,110,111,112,114,115,116,117,118,119]
def trigger_randtpch(): 
  qtime = {}
  if os.path.isfile(QTIME_TPCH):
    with open(QTIME_TPCH) as f:
      reader = csv.DictReader(f)
      for line in reader:
        qtime[line['tpch_qid']] = line['qtime']
  jobs = deque()
  for i in qids:
    if 'q%d'%i not in qtime:
      jobs.append({'triggermode':'no', 'outf':QTIME_TPCH, 'tpch_qid':'q%d'%i})
  oldcount = threading.active_count()
  while True: 
    if len(jobs) > 0:
      job = jobs.popleft()
      one_expr_thread(job, pick_one_instance('tpch'), None).start()
    if len(jobs) == 0 and threading.active_count() == oldcount: break
    time.sleep(1)
  for j in range(150):
    for i in qids:
      jobs.append({'triggermode':int(float(qtime['q%d'%i])), 'outf':'rand_tpch_%d'%i, 'tpch_qid':'q%d'%i})
  oldcount = threading.active_count()
  while True: 
    if len(jobs) > 0:
      job = jobs.popleft()
      one_expr_thread(job, pick_one_instance('tpch'), None).start()
    if len(jobs) == 0 and threading.active_count() == oldcount: break
    time.sleep(1)

def trigger_randtime():
  qtime = load_qtime()
  while True:
    totalq = 0
    for longc in range(dimen('long', 'min'), dimen('long', 'max')+1):
      if op == 'aggjoin' and longc == dimen('long', 'max'): continue
      for strc in range(dimen('str', 'min'), dimen('str', 'max')+1):
        for strsum in [0] if strc == 0 else [4,20,36,52,68]:
          coor = {'long':longc, 'str':strc, 'strsum':strsum, 'nT':dimen('nT', 'max'), 'nK':dimen('nK', 'max'), \
                  'nT_delta':dimen('nT_delta', 'max'), 'nK_delta':dimen('nK_delta', 'max'), 'triggermode':'no', 'outf':QTIME_HASH}
          if valid(coor) != 0: continue
          if formkey(coor) in qtime: continue
          jobs.append(coor)
    wait_until_done('trigger')
    qtime = load_qtime()
    if len(qtime) == totalq: break
  print 'done with qtime'

  for i in range(7):
    for longc in range(dimen('long', 'min'), dimen('long', 'max')+1):
      if op == 'aggjoin' and longc == dimen('long', 'max'): continue
      for strc in range(dimen('str', 'min'), dimen('str', 'max')+1):
        for strsum in [0] if strc == 0 else [4,20,36,52,68]:
          coor = {'long':longc, 'str':strc, 'strsum':strsum, 'nT':dimen('nT', 'max'), 'nK':dimen('nK', 'max'), \
                  'nT_delta':dimen('nT_delta', 'max'), 'nK_delta':dimen('nK_delta', 'max'), 'outf':QTIME_HASH}
          if valid(coor) != 0: continue
          k = formkey(coor)
          if k not in qtime:
            print k, 'not in qtime'
            continue
          coor['triggermode'] = int(float(qtime[k][0]))
          jobs.append(coor)
  wait_until_done('trigger')

def main(argv):
  action = argv[1]
  if action == 'gengrid':
    gen_points('grid')
  elif action == 'genrand':
    gen_points('rand')
  elif action == 'train':
    load_past()
    push_exprs(GRID_POINTS, GRID_OUT)
    push_exprs(RAND_POINTS, RAND_OUT)
    wait_until_done('train')
  elif action == 'trigger':
    trigger_randtime()
  elif action == 'tpch':
    trigger_tpch()

def dimen(k, v):
  if k in ['long']:
    if v == 'max': return 7
    elif v == 'min': return 1
    elif v == 'grid': return 2
    elif v == 'gridlen': return (7-1)/2
  if k in ['str']:
    if v == 'max': return 8
    elif v == 'min': return 0
    elif v == 'grid': return 2
    elif v == 'gridlen': return (8-0)/2
  if k == 'strsum':
    if v == 'max': return 96
    elif v == 'min': return 0
    elif v == 'grid': return 4
    elif v == 'gridlen': return (96-0)/4
  if k in ['nT', 'nT_delta', 'nK', 'nK_delta']:
    if v == 'max': return 1200 * qplan.TB
    elif v == 'min': return 0
    elif v == 'grid': return 4
    elif v == 'gridlen': return (1200-0)/4 * qplan.TB

def gen_grid(step, cnt, coors):
  if step == len(dimens):
    if valid(cnt) == 0:
      coors.append(copy.deepcopy(cnt))
    return
  k = dimens[step]
  for v in range(dimen(k, 'grid')+1):
    cnt[k] = dimen(k, 'min') + dimen(k, 'gridlen') * v
    if k in ['nT', 'nK']:
      if cnt[k] == 0: cnt[k] = 1 
    gen_one_step_grid(step+1, cnt, coors)

def gen_rand(step, cnt, coors):
  for t in range(2):
    with open(GRID_POINTS) as f:
      reader = csv.DictReader(f)
      for line in reader:
        for i in range(1000):
          cnt = {}
          for k in line:
            cnt[k] = int(line[k]) + random.randint(0, dimen(k, 'gridlen')-1)
          if valid(cnt) == 0:
            coors.append(cnt)
            break

def gen_points(mode):
  coors = []
  if mode == 'grid':
    fout = GRID_POINTS
    gen_one_step_grid(0, {}, coors)
  elif mode == 'rand':
    fout = RAND_POINTS
    gen_one_step_rand(0, {}, coors)
  with open(fout, 'w') as f:
    writer = csv.DictWriter(f, fieldnames=dimens)
    writer.writeheader()
    for c in coors:
      writer.writerow(c)

def wait_until_done(tag=None):
  pre_count = 0
  oldcount = threading.active_count()
  while True: 
    if len(jobs) > 0:
      job = jobs.popleft()
      expr_id = did_expr(job) 
      if expr_id == None:
        print 'popped', job, len(jobs), expr_id
        one_expr_thread(job, pick_one_instance(tag), None).start()
    if len(jobs) == 0 and threading.active_count() == oldcount: break
    if len(jobs) == 0:
      cnt_count = threading.active_count()
      if cnt_count == pre_count:
        if time.time() - timestamp > 240: break
      else:
        timestamp = time.time()
        pre_count = cnt_count
      time.sleep(0.2)

def load_past():
  for outf in [GRID_OUT, RAND_OUT]:
    if not os.path.isfile(outf): continue
    with open(outf) as f:
      reader = csv.DictReader(f)
      for line in reader:
        t = {'nT':int(float(line['nT'])), 'nK':int(float(line['nK'])), 'nT_delta':int(float(line['nT_delta'])), 'nK_delta':int(float(line['nK_delta']))}
        for k in line:
          if k.endswith('long'): t['long'] = int(line[k])
          if k.endswith('str'): t['str'] = int(line[k])
          if k.endswith('strsum'): t['strsum'] = int(line[k])
        exprs.append(t)
  print 'loaded past experiments', len(exprs)

if __name__ == "__main__":
  main(sys.argv)
