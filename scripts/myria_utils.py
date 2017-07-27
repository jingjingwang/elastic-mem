#!/usr/bin/env python
import json
import time
import random
import os.path
import subprocess
import requests
import re
import csv
import numpy
import sys
import jvm
import logging
from threading import Thread

logging.getLogger("urllib3").setLevel(logging.WARNING)
random.seed()
myriadeploy = "/data/myria/myriadeploy"
working_instances = {}
datapath = "/data/" 

def url(hostname, port):
  return "http://%s:%s/" % (hostname, str(port))

def ssh_call(hostname, args):
  if hostname != 'localhost':
    conn = ' '.join(["ssh", "-i", "jwang.pem", "ec2-user@" + hostname])
  else:
    conn = ''
  subprocess.call(conn + ' ' + args, shell=True)

def kill_keyword(hostname, keyword):
  cmd = "ps -u ec2-user -o pid,command | grep -E \'%s\' | grep -v grep | awk \"{print \\$1}\"" % (keyword)
  pids = filter(None, subprocess.check_output(cmd, shell=True).split('\n'))
  print 'killing', ' '.join(pids), keyword
  for pid in pids:
    ssh_call(hostname, "kill -9 %s" % pid)

def kill_java_python(hostname, py=False):
  keyword = 'bin/java|python' if py else 'bin/java'
  cmd = "ps -u ec2-user -o pid,command | grep -E \'%s\' | grep -v grep | awk \"{print \\$1}\"" % (keyword)
  pids = filter(None, subprocess.check_output(cmd, shell=True).split('\n'))
  print 'killing', ' '.join(pids)
  for pid in pids:
    ssh_call(hostname, "kill -9 %s" % pid)

def query_status(hostname, rest, qid):
  try:
    r = requests.get(url(hostname, rest) + "query/query-%s" % qid).json()
  except requests.exceptions.ConnectionError:
    return None
  return r

def query_elapsed(hostname, rest, qid):
  try:
    r = requests.get(url(hostname, rest) + "query/query-%s" % qid).json()["elapsedNanos"]
  except requests.exceptions.ConnectionError:
    return 0.0
  try:
    return float(r) / 1000000000.0
  except TypeError:
    return 0.0

def query_finished(hostname, rest, qid):
  try:
    s = requests.get(url(hostname, rest) + "query/query-%s" % qid).json()["status"]
  except requests.exceptions.RequestException as e:
    return 0
  if s == "SUCCESS":
    return 1
  if s == "ERROR" or s == "KILLED" or s == "UNKNOWN":
    return 2
  return -1

def block_until_finish(hostname, rest, qid):
  while query_finished(hostname, rest, qid) < 0:
    time.sleep(5)

def launch(host, deploy):
  if host != 'localhost':
    conn = 'ssh -i %s ec2-user@%s' % (jwang.pem, host)
  else:
    conn = ''
  subprocess.Popen(' '.join([conn, "cd", myriadeploy, "&&", "nohup ./launch_local_cluster", deploy['name'], "> /dev/null 2>&1", "&"]), shell=True)
  time.sleep(2)
  while True:
    try:
      if len(requests.get(url(host, deploy['master_rest']) + "workers/alive").json()) > 0: break
    except requests.exceptions.ConnectionError:
      pass
    time.sleep(1)

def shutdown(hostname, myriadeploy):
  ssh_call(hostname, ["\"cd", myriadeploy, "&&", "./stop_all_by_force.py", '"'])
  time.sleep(1)

def hash_table_stats(host, rest, qid):
  try:
    r = requests.get(url(host, rest) + 'hashtable-query-%s' % qid, timeout=0.1)
  except requests.exceptions.Timeout:
    return 'timeout'
  except requests.exceptions.ConnectionError:
    return None
  try:
    r.raise_for_status()
  except requests.exceptions.HTTPError:
    return None
  return r.text

def working_dir(host, rest):
  try:
    r = requests.get(url(host, rest) + 'working_dir')
  except requests.exceptions.ConnectionError:
    return ""
  return r.text

def get_rss_gen_max(host, deploy):
  launch(host, deploy)
  time.sleep(1)
  requests.get(url(host, deploy['workers'][0][3]) + "sysgc")
  time.sleep(1)
  ret = jvm.get(host, deploy['workers'][0][4], ['eused', 'oused']).strip().split('|')
  jvm.send(host, deploy['workers'][0][4], ['emax=%s' % ret[0], 'smax=0', 'omax=%s' % ret[1], 'doneinit'])
  time.sleep(1)
  initrss = int(jvm.get(host, deploy['workers'][0][4], ['rss'])[:-1])
  emax, omax = jvm.get(host, deploy['workers'][0][4], ['emax', 'omax']).strip().split('|')[:2]
  return (initrss, emax, omax)

def pre_submit_plan(host, deploy):
  for vm in deploy['workers']:
    requests.get(url(host, vm[3]) + "sysgc")
    time.sleep(1)
    if 'myjava' in deploy['name']: 
      eused, oused = jvm.get(host, vm[4], ['eused', 'oused']).strip().split('|')[:2]
      jvm.send('localhost', vm[4], ['smax=0', 'doneinit'])
      #jvm.send('localhost', vm[4], ['smax=0', 'emax=%s' % eused, 'omax=%s' % oused, 'doneinit'])

def get_shrink_rss_before_query(host, port):
  ret = jvm.get(host, port, ['shrink_rss_count', 'shrink_rss_size']).strip().split('|')[:2]
  return (int(ret[0]), int(ret[1]))

def get_expand_before_query(host, port):
  ret = jvm.get(host, port, ['expand_count', 'expand_size']).strip().split('|')[:2]
  return (int(ret[0]), int(ret[1]))

def submit_plan(plan, hostname, rest):
  print 'submit plan', hostname, rest
  r = requests.post(url(hostname, rest) + "query", data=json.dumps(plan), headers={'Content-Type':'application/json'})
  print r.text
  qid = str(r.json()["queryId"])
  print "running query %s on %s %d" % (qid, hostname, rest)
  return qid

def parse(instance_id, hostname, qid, confs, gclogfile, stdoutfile):
  minor_gc = []
  major_gc = []
  latest_major_stats = {}
  latest_minor_stats = {}
  with open(gclogfile) as f:
    for line in f.readlines():
      line = line.strip()
      if line.find("[GC") >= 0:
        region = filter(None, re.split("\[|\]|:| |,|=", line))
        latest_minor_stats = {"timestamp": float(region[0]), "pf":line.find("[GC--") >= 0}
        latest_minor_stats.update(get_times(region[9], region[11], region[13], "y"))
        minor_gc.append(latest_minor_stats);
      elif line.find("[Full GC") >= 0:
        region = filter(None, re.split("\[|\]|:| |,|=", line))
        latest_major_stats = {"timestamp": float(region[0])}
        latest_major_stats.update(get_times(region[14], region[16], region[18], "o"))
        major_gc.append(latest_major_stats);
      elif line.startswith("ObjectCountDeltaFull"):
        latest_major_stats.update(add_obj_count(line));
      elif line.startswith("ObjectCountDeltaYoung"):
        latest_minor_stats.update(add_obj_count(line));
  if len(major_gc) != len(minor_gc):
    with open("extra_gc_confs", "a") as f:
      f.write("\t".join([instance_id, hostname, qid, json.dumps(confs)]) + "\n")
    return []
  if len(major_gc) <= 1:
    return []

  i = 1 # 1st gc does not have opstats since no query
  triggers = []
  with open(stdoutfile) as f:
    for line in f.readlines():
      if line.startswith("sysgc "):
        triggers.append(int(line.split(' ')[1].strip()))
      if line.startswith("hash_table_stats"):
        tokens = filter(None, line.strip().split('\t'))[1:-1]
        stats = {}
        for token in tokens:
          (k, v) = token.split(' ') 
          stats[k] = float(v)
          if i > 1: # taking delta for everyting
            stats[k+"_delta"] = stats[k] - major_gc[i-1][k]
          else:
            stats[k+"_delta"] = stats[k]
        major_gc[i].update(stats)
        i += 1
  if len(major_gc) != len(triggers):
    with open("extra_gc_confs", "a") as f:
      f.write("\t".join([instance_id, hostname, qid, json.dumps(confs)]) + "\n")
    return []
  major_gc = major_gc[:i]
  minor_gc = minor_gc[:i]
  triggers = triggers[:i]

  lines = []
  prev_time = prev_count = prev_size = actual_time = 0
  for i, major in enumerate(major_gc):
    minor = minor_gc[i]
    #actual_time += minor["timestamp"] - minor["pre_count_time"] - prev_time; # since JVM excluding gc
    yacount, yasize = minor["pre_count"] - prev_count, minor["pre_size"] - prev_size
    ydcount, ydsize = max(minor["pre_count"] - minor["post_count"], 0), max(minor["pre_size"] - minor["post_size"], 0)
    ylcount, ylsize = yacount - ydcount, yasize - ydsize
    odcount, odsize = max(major["pre_count"] - major["post_count"], 0), max(major["pre_size"] - major["post_size"], 0)
    olcount, olsize = prev_count - odcount, prev_size - odsize
    prev_time = major["timestamp"] + major["oreal"] + major["post_count_time"];
    prev_count = major["post_count"]
    prev_size = major["post_size"]
    line = {"instance_id":instance_id, "hostname":hostname, "confs":json.dumps(confs), "qid": qid, "time":minor['time'], "ylcount":ylcount, "ylsize":ylsize, "ydcount":ydcount, "ydsize":ydsize, \
            "olcount":olcount, "olsize":olsize, "odcount":odcount, "odsize":odsize, "trigger":triggers[i]}
    line.update(dict((k, v) for k, v in minor.iteritems() if not (k.startswith("pre_") or k.startswith("post_"))))
    line.update(dict((k, v) for k, v in major.iteritems() if not (k.startswith("pre_") or k.startswith("post_"))))
    lines.append(line)
  return lines

def parsegcline(line, prefix):
  #line = line.translate(None, '\x00')
  tokens = filter(None, line.split()) #re.split("\[|\]|:| |,|=|->|\(|\)|K", line))
  result = {'time':float(tokens[0][:-1])}
  for t in tokens:
    if t.startswith('user'):
      result[prefix+"user"] = float(t.split('=')[1])
    elif t.startswith('sys'):
      result[prefix+"sys"] = float(t.split('=')[1][:-1])
    elif t.startswith('real'):
      result[prefix+"real"] = float(t.split('=')[1])
    elif '->' in t:
      sizes = filter(None, re.split('->|K', t))
      if 'ypresize' not in result:
        result['ypresize'] = int(sizes[0])
        result['ypostsize'] = int(sizes[1])
      elif 'opresize' not in result:
        result['opresize'] = int(sizes[0])
        result['opostsize'] = int(sizes[1])
  if 'opostsize' not in result: print line, result
  return result
  #return {prefix+"user":float(region[18]), prefix+"sys":float(region[20]), prefix+"real":float(region[22]), \
  #        "time":float(region[0]), "ypresize":int(region[4]), "ypostsize":int(region[5]), "opresize":int(region[8]), "opostsize":int(region[9])}

def drop_cache(s):
  ssh_call(s, 'sudo sh -c \"echo 3 > /proc/sys/vm/drop_caches\"')
