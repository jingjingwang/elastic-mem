#!/usr/bin/env python

import json
import csv
import sys
import subprocess
from collections import deque

def formkey(line):
  return '_'.join([line['which'], line['memincre'], line['runningqlen'], line['resubmit'], str(line['totalmem']), str(line['delayrange']), str(line['roundsleep'])])

result = {}
files = [
'scheduler_2g_ref.csv',
'scheduler.30s_15.csv',
'scheduler.30s_20.csv',
'scheduler.30s_25.csv',
'scheduler.30s_30.csv',
'scheduler.30s_40.csv',
'scheduler.30s_50.csv',
'scheduler.30s_70.csv',
'scheduler.30s_90.csv',
'scheduler.30s.csv',
'scheduler.inter_500.csv',
'scheduler.inter_500_1.csv',
'scheduler.inter.csv',
'scheduler.inter_12_1.csv'
]
trial = 5

with open('scheduler_2g_ref_new.csv', 'w') as fout:
  header = []
  for f in files:
    with open(f) as fin: 
      reader = csv.DictReader(fin)
      if header == []: header = reader.fieldnames
      for line in reader:
        k = formkey(line)
        if k not in result: result[k] = deque()
        if len(result[k]) == trial: result[k].popleft()
        result[k].append(line)
  ref_elapsed, ref_allqtime, ref_allgctime = {}, {}, {}
  for k in result:
    for line in result[k]:
      if line['which'] == 'ori' and line['runningqlen'] == '8' and line['delayrange'] == '0':
        if line['totalmem'] not in ref_elapsed:
          ref_elapsed[line['totalmem']] = [0, 0]
          ref_allqtime[line['totalmem']] = [0, 0]
          ref_allgctime[line['totalmem']] = [0, 0]
        ref_elapsed[line['totalmem']][0] += float(line['elapsed'])
        ref_allqtime[line['totalmem']][0] += float(line['allqtime'])
        ref_allgctime[line['totalmem']][0] += float(line['allgctime'])
        ref_elapsed[line['totalmem']][1] += 1
        ref_allqtime[line['totalmem']][1] += 1
        ref_allgctime[line['totalmem']][1] += 1
  header1 = []
  for h in header: 
    if not h.startswith('ref_'): header1.append(h)
  header = header1
  print header
  for k in result:
    for line in result[k]:
      line['ref_elapsed'] = ref_elapsed[line['totalmem']][0] / ref_elapsed[line['totalmem']][1]
      line['ref_allqtime'] = ref_allqtime[line['totalmem']][0] / ref_allqtime[line['totalmem']][1]
      line['ref_allgctime'] = ref_allgctime[line['totalmem']][0] / ref_allgctime[line['totalmem']][1]
  writer = csv.DictWriter(fout, fieldnames=header + ['ref_elapsed', 'ref_allqtime', 'ref_allgctime'])
  writer.writeheader()
  for k in sorted(result):
    if len(result[k]) != trial and 'False' in k:
      print k, len(result[k])
    for line in result[k]:
      writer.writerow(line)

#subprocess.call('mv scheduler_2g_ref_new.csv scheduler_2g_ref.csv', shell=True)
