#!/usr/bin/env python
import json
import time
import random
import os.path
import subprocess
import requests
import re
import sys
import copy

def fill(s, l):
  s = str(s)
  for i in range(l - len(s)):
    s += "a"
  return s

def extend(q1):
  q = copy.deepcopy(q1) 
  if 'extended' in q and q['extended']: return q
  if 'par' in q:
    sump = maxp = 0
    for p in q['par']:
      sump += p
      maxp = max(maxp, p)
    for k in ['nT', 'nT_delta', 'nK', 'nK_delta']: q[k] = int(q[k] * 1.0 * sump / maxp)
  q['extended'] = True
  return q

def genfilename(q, par=None):
  q = extend(q)
  args = [str(q['nT']), str(q['nT_delta']), str(q['nK']), str(q['nK_delta']), q['schema']]
  if par is not None: args.append(str(par))
  p = '/data/' + ('q%d/' % q['qindex'] if 'qindex' in q else '') 
  subprocess.call('mkdir -p %s' % p, shell=True)
  return p + '_'.join(args)

def genline(i, maxk, newk, types):
  types = types.split('_')
  line = ""
  for col in range(len(types)):
    value = 0
    if col == 0: #  the key column, don't mess it up
      if i < newk:
        value = i + maxk - newk
      else: 
        assert maxk >= 1
        value = random.randint(0, maxk-1) 
    else:
      line += ","
    if types[col].startswith('S'):
      line += fill(value, int(types[col][1:]))
    else:
      line += str(value)
  return line

def gen(q):
  filename = genfilename(q)
  q = extend(q)
  print "generating", q, filename
  if not os.path.isfile(filename):
    temp = "/data/tempdata%d" % random.randint(0, 1000000)
    with open(temp, "w") as f:
      for i in range(q['nT'] - q['nT_delta']):
        f.write(genline(i, q['nK'] - q['nK_delta'], q['nK'] - q['nK_delta'], q['schema']) + '\n')
      for i in range(q['nT_delta']):
        f.write(genline(i, q['nK'], q['nK_delta'], q['schema']) + '\n')
    subprocess.call("mv %s %s" % (temp, filename), shell="True")
  if 'par' not in q: return
  sump = 0
  for p in q['par']: sump += p
  need = False
  for i in range(len(q['par'])):
    filename = genfilename(q, i)
    if not os.path.isfile(filename):
      need = True
      break
  if not need: return
  outputs = []
  for i in range(len(q['par'])): outputs.append([])
  filename = genfilename(q)
  with open(filename) as f:
    for line in f.readlines():
      key = int(line.split(',')[0]) % sump
      currentp = 0
      for idx, p in enumerate(q['par']):
        currentp += p
        if key < currentp:
          outputs[idx].append(line)
          break
  for i in range(len(q['par'])):
    filename = genfilename(q, i)
    if os.path.isfile(filename): continue
    temp = "/data/tempdata%d" % random.randint(0, 1000000)
    with open(temp, 'w') as f:
      for line in outputs[i]:
        f.write(line)
    subprocess.call("mv %s %s" % (temp, filename), shell="True")

def main(argv):
  gen({'nT':int(argv[1]), 'nT_delta':int(argv[2]), 'nK':int(argv[3]), 'nK_delta':int(argv[4]), 'schema':argv[5]})

if __name__ == "__main__":
  main(sys.argv)
