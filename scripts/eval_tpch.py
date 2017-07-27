#!/usr/bin/env python
import json
import bisect
import copy
import math
import time
import random
import os.path
import subprocess
import requests
import re
import csv
import sys
import weka.core.jvm as jvm
import weka.core.converters as converters
import weka.core.serialization as serialization
from weka.core.classes import Random
from weka.core.dataset import Instance
from weka.classifiers import Classifier, Evaluation
from operator import itemgetter
from adap_train import valid
from scheduler import parse_opstats
import qplan
from manual_model import manual_pred

random.seed()
EPS = 0.000001
op = None

def pfloat(a):
  try:
    x = float(a)
    return x
  except ValueError:
    return None

def addf():
  return [
  'nTdeltaLong', 'nTdeltaStr', 'nTdeltaLogStrSum', 'nTdeltaStrSum', 
  'nTLong', 'nTStr', 'nTStrSum', 'nTLogStrSum',
  'nTPreLong', 'nTPreStr', 'nTPreStrSum', 'nTPreLogStrSum'
]

def getpara(name, t):
  if t == 'op':
    return ['%s_nK' % name, '%s_nK_delta' % name, '%s_nT' % name, '%s_nT_delta' % name, \
            '%s_long' % name, '%s_str' % name, '%s_strsum' % name]
  if t == 'grid':
    return ['nK', 'nK_delta', 'nT', 'nT_delta', 'long', 'str', 'strsum']
  if t == 'new':
    return ['%s_%s' % (name, k) for k in addf()]
  if t == 'heaptime':
    return ['oreal', 'yreal', 'olsize', 'ylsize', 'ydsize', 'odsize']

def model_file(op, obj):
  return "models/"+','.join([op, obj])

def eval_one_split(traindata, testdata, obj):
  c = Classifier(classname="weka.classifiers.trees.M5P")
  c.build_classifier(traindata)
  objidx = traindata.attribute_by_name(obj).index
  preds = []
  reals = []
  for idx, ins in enumerate(testdata):
    pred = c.classify_instance(ins)
    real = ins.get_value(objidx)
    preds.append(pred)
    reals.append(real)
    header = ''
    for h in traindata.attributes(): header += ' ' + h.name
  return preds, reals
    
def split(data, testf, label, fold):
  header = []
  for a in data.attributes():
    header.append(a.name)
  suffix = random.randint(0, 1000000)
  trainfile = 'temptrain_%d.csv' % suffix
  testfile = 'temptest_%d.csv' % suffix
  testidx = []
  with open(trainfile, 'w') as ftrain, open(testfile, 'w') as ftest:
    ftrain.write(','.join(header) + '\n')
    ftest.write(','.join(header) + '\n')
    for idx, ins in enumerate(data):
      if testf(label[idx], fold): # more ways 
        ftest.write(str(ins) + '\n')
        testidx.append(idx)
      else:
        ftrain.write(str(ins) + '\n')
  return trainfile, testfile, testidx

def istest_10fold(label, fold):
  return label == fold

def istest_oneout(idx, outidx):
  return idx == outidx

def cleanup(f, attrs, obj):
  data = converters.load_any_file(f)
  n = data.num_attributes
  for idx in range(n):
    data.delete_with_missing(idx)
  for idx in reversed(range(n)):
    if data.attribute(idx).name not in attrs and data.attribute(idx).name != obj:
      data.delete_attribute(idx)
  for idx in range(data.num_attributes):
    if data.attribute(idx).name == obj:
      data.class_index = idx
  return data

def metric(ps, rs, data = None, debug = False):
  assert(len(ps) == len(rs))
  avg_r = 0;
  for r in rs:
    avg_r += float(r)
  avg_r /= len(rs)
  s1 = s2 = s3 = s4 = 0.0
  for idx, p in enumerate(ps):
    s4 += abs(avg_r - rs[idx])
    s2 += (avg_r - rs[idx]) ** 2
  #print avg_r, s4, len(ps)
  if s2 < EPS: s2 = EPS
  if s4 < EPS: s4 = EPS
  for idx, p in enumerate(ps):
    s1 += (p - rs[idx]) ** 2
    s3 += abs(p - rs[idx])
    if debug and abs(p - rs[idx]) / rs[idx] > 6: #abs(p - rs[idx]) / s4 * 100 > 1.5:
      if data is not None: print '   ', idx, data.get_instance(idx)
      header = ''
      for h in data.attributes(): header += ' ' + h.name
      #print header
      #print idx, 'pred', p, 'real', rs[idx], 'pratio', abs(p - rs[idx]) / rs[idx], 'diffratio', abs(p - rs[idx]) / s4, 'totalratio', s3 / s4
    #if debug: print idx, 'pred', p, 'real', rs[idx], 'pratio', abs(p - rs[idx]) / rs[idx], abs(p - rs[idx]) / s4, 's3', s3
  return (str(s3 / s4 * 100), str((s1 / s2) ** 0.5 * 100))

def train(objs, paras, outfiles):
  outfile = preprocess(outfiles)
  print 'train', objs, paras, outfile
  data = converters.load_any_file(outfile)
  preds = {}
  reals = {}
  for obj in objs:
    preds[obj] = []
    reals[obj] = []
  label = []
  testidxes = []
  for idx, ins in enumerate(data):
    label.append(random.randint(0, 9))
  for i in range(10):
    trainfile, testfile, testidx = split(data, istest_10fold, label, i)
    for obj in objs:
      traindata = cleanup(trainfile, paras, obj)
      testdata = cleanup(testfile, paras, obj)
      pred, real = eval_one_split(traindata, testdata, obj)
      preds[obj].extend(pred)
      reals[obj].extend(real)
    testidxes.extend(testidx)
    subprocess.call('rm %s %s' % (trainfile, testfile), shell=True)
  subprocess.call('rm %s' % outfile, shell=True)
  print 'num ins', data.num_instances
  for obj in objs:
    print obj, metric(preds[obj], reals[obj])
  return data, preds, reals, testidxes

def output_model(objs, paras, outfiles):
  global op
  outfile = preprocess(outfiles)
  c = Classifier(classname="weka.classifiers.trees.M5P")
  for obj in objs:
    data = cleanup(outfile, paras, obj)
    print 'output_model', op, obj, paras, data.num_instances, outfile
    header = []
    for a in data.attributes():
      header.append(a.name)
    c.build_classifier(data)
    #print c
    serialization.write(model_file(op, obj), c)
    #e = Evaluation(data)
    #e.test_model(c, data)
    #print e.summary()
  subprocess.call('rm %s' % outfile, shell=True)

def get_lrvalue(paras, ins, dimens, data, mode):
  ret = {}
  for para in paras:
    value = int(getvalue(ins, para, data))
    pleft, pright = bisect.bisect_left(dimens[para], value), bisect.bisect_right(dimens[para], value)
    if pright >= len(dimens[para]): pright = len(dimens[para])-1
    vleft, vright = dimens[para][pleft], dimens[para][pright]
    if mode == 'grid':
      if pleft > 0: vleft = dimens[para][pleft-1]
    if mode == 'rand':
      if dimens[para][pleft] != value: # > then
        if pleft > 0: vleft = dimens[para][pleft-1]
    if vleft == vright: ret[para] = [vleft]
    else: ret[para] = [vleft, vright]
  return ret

def get_neighbors(neighbors, level, coor, points):
  if level == len(neighbors.keys()):
    points.append(copy.deepcopy(coor))
    return
  para = sorted(list(neighbors.keys()))[level]
  for value in neighbors[para]:
    coor[para] = value
    get_neighbors(neighbors, level+1, coor, points)

def did_expr(coor, exprs):
  for idx, e in enumerate(exprs):
    same = True
    for k in coor:
      if coor[k] != e[k]: same = False
    if same:
      return True
  return False

def getvalue(ins, attr, data, mode='number'):
  if mode == 'number':
    return ins.get_value(data.attribute_by_name(attr).index)
  return ins.get_string_value(data.attribute_by_name(attr).index)

def add_features(line, op):
  nTPre = float(line['%s_nT' % op]) - float(line['%s_nT_delta' % op])
  line['%s_nTdeltaLong' % op] = float(line['%s_nT_delta' % op]) * float(line['%s_long' % op])
  line['%s_nTdeltaStr' % op] = float(line['%s_nT_delta' % op]) * float(line['%s_str' % op])
  line['%s_nTdeltaStrSum' % op] = float(line['%s_nT_delta' % op]) * float(line['%s_strsum' % op])
  line['%s_nTdeltaLogStrSum' % op] = float(line['%s_nT_delta' % op]) * math.log(float(line['%s_strsum' % op])+1)
  line['%s_nTLong' % op] = float(line['%s_nT' % op]) * float(line['%s_long' % op])
  line['%s_nTStr' % op] = float(line['%s_nT' % op]) * float(line['%s_str' % op])
  line['%s_nTStrSum' % op] = float(line['%s_nT' % op]) * float(line['%s_strsum' % op])
  line['%s_nTLogStrSum' % op] = float(line['%s_nT' % op]) * math.log(float(line['%s_strsum' % op])+1)
  line['%s_nTPreLong' % op] = nTPre * float(line['%s_long' % op])
  line['%s_nTPreStr' % op] = nTPre * float(line['%s_str' % op])
  line['%s_nTPreStrSum' % op] = nTPre * float(line['%s_strsum' % op])
  line['%s_nTPreLogStrSum' % op] = nTPre * math.log(float(line['%s_strsum' % op])+1)

def preprocess(outfiles, test=False):
  prepoutfile = 'prep_%d.csv' % (random.randint(0, 10000))
  with open(prepoutfile, 'w') as fout:
    header = []
    for outfile in outfiles:
      with open(outfile) as fin:
        reader = csv.DictReader(fin)
        if len(header) == 0:
          header = reader.fieldnames
          if not test:
            header += ['hash_%s' % k for k in addf()]
          writer = csv.DictWriter(fout, fieldnames=header)
          writer.writeheader()
        for line in reader:
          if not test:
            add_features(line, 'hash')
          writer.writerow(line)
  return prepoutfile

def test_manual(objs, paras, otestfile, pred, real):
  testfile = preprocess(otestfile, True)
  with open(testfile) as fin: #, open('perhashtable.csv','a') as fout:
    reader = csv.DictReader(fin)
    #writer = csv.DictWriter(fout, fieldnames=paras + ['op','linenum','testfile'] + objs)
    #writer.writeheader()
    linecount = 0
    for idx, line in enumerate(reader):
      stats = {}
      valid = True
      real_line = {}
      for h in line:
        if h.startswith('op'):
          k = h[:h.find('_')]
          v = h[h.find('_')+1:]
          if k not in stats: stats[k] = {}
          stats[k][v] = pfloat(line[h])
          if stats[k][v] is None:
            valid = False
            break;
        elif h in objs:
          real_line[h] = pfloat(line[h])
          if real_line[h] is None:
            valid = False
            break;
      if not valid:
        print 'invalid', line
        continue
      linecount += 1
      if linecount > 250: continue
      for k in stats:
        assert len(paras) == len(stats[k])
        for v in stats[k]: assert v in paras
      zeroref = {'nT':1,'nT_delta':0,'nK':1,'nK_delta':0,'long':1,'str':0,'strsum':0}
      for obj in objs:
        s = manual_pred(obj, zeroref)
        for op in stats:
          prediction = manual_pred(obj, stats[op])
          s = s + max(prediction  - manual_pred(obj, zeroref), 0)
        pred[obj].append(s)
        real[obj].append(real_line[obj])
  print 'test', testfile, 'linecount', linecount
  subprocess.call('rm %s' % testfile, shell=True)

def test(objs, paras, testfile1, pred, real):
  testfile = preprocess(testfile1, True)
  xref = {'x_nT':1,'x_nT_delta':0,'x_nK':1,'x_nK_delta':0,'x_long':1,'x_str':0,'x_strsum':0}
  add_features(xref, 'x')
  zeroref = []
  for k in ['long', 'nK', 'nK_delta', 'nT', 'nT_delta', 'str', 'strsum']:
    zeroref.append(xref['x_%s' % k])
  zeroref.append(0) # should be obj
  for k in addf():
    zeroref.append(xref['x_%s' % k])

  with open(testfile) as fin:
    reader = csv.DictReader(fin)
    linecount = 0
    for line in reader:
      ops = []
      for h in line:
        if h.startswith('op'): ops.append(h[:h.find('_')])
      for op in ops: add_features(line, op)
      stats = {}
      valid = True
      real_line = {}
      for h in line:
        if h.startswith('op'):
          k = h[:h.find('_')]
          v = h[h.find('_')+1:]
          if k not in stats: stats[k] = {}
          stats[k][v] = pfloat(line[h])
          if stats[k][v] is None:
            valid = False
        elif h in objs:
          real_line[h] = pfloat(line[h])
          if real_line[h] is None:
            valid = False
      if not valid: continue
      linecount += 1
      if linecount > 250: continue
      #for k in stats:
      #  assert len(paras) == len(stats[k])
      #  for v in stats[k]:
      #    assert v in paras
      for obj in objs:
        c = Classifier(jobject=serialization.read(model_file('hash', obj)))
        zerovalue = c.classify_instance(Instance.create_instance(zeroref))
        #s = 0
        s = zerovalue
        for op in stats:
          values = []
          for k in ['long', 'nK', 'nK_delta', 'nT', 'nT_delta', 'str', 'strsum']:
            values.append(stats[op][k])
          values.append(0) # should be obj
          for k in addf():
            values.append(stats[op][k])
          ins = Instance.create_instance(values)
          prediction = c.classify_instance(ins)
          #print '   ', obj, op, values, prediction, prediction - zerovalue
          #s += pred
          s = s + max(prediction - zerovalue, 0)
        #print obj, 'real', real_line[obj], 'pred', s
        pred[obj].append(s)
        real[obj].append(real_line[obj])
  print 'test', testfile, 'linecount', linecount
  subprocess.call('rm %s' % testfile, shell=True)


def remove_op(s, op):
  if s.startswith(op): s = s[s.find('_')+1:]
  return s

def test_single():
  #['long', 'nK', 'nK_delta', 'nT', 'nT_delta', 'str', 'strsum']:
  objs = ['olsize', 'ylsize']
  for obj in objs:
    c = Classifier(jobject=serialization.read(model_file('hash', obj)))
    values = [3.0, 192.0, 124.0, 192.0, 124.0, 6.0, 144.0]
    values.append(0) # should be obj
    ins = Instance.create_instance(values)
    prediction = c.classify_instance(ins)
    print obj, prediction

def main(argv):
  if len(argv) <= 1:
    print 'op action testfile/batch'
    return
  jvm.start()
  global op
  op = argv[1]
  action = argv[2]
  if action == "train":
    train(getpara(op, 'heaptime'), getpara(op, 'op')+getpara('hash', 'new'), argv[3:])
  if action == "output_model":
    output_model(getpara(op, 'heaptime'), getpara(op, 'op')+getpara('hash', 'new'), argv[3:])
  if action == "test": 
    pred = {}
    real = {}
    objs = getpara(op, 'heaptime')
    for obj in objs:
      pred[obj] = []
      real[obj] = []
    #for i in [101,102,103,104,105,106,108,109,110,111,112,114,115,116,117,118,119]:
    for i in [1,2,3,4,5,6,8,9,10,11,12,14,15,16,17,18,19] + [101,102,103,104,105,106,108,109,110,111,112,114,115,116,117,118,119]:
    #for i in [4,5,8,9,10,12,18,19]:
      test(objs, getpara(op, 'grid'), ['randtime_tpch_%d.csv' % i], pred, real)
    for obj in objs:
      print obj, metric(pred[obj], real[obj]), len(pred[obj])
  if action == "test_manual": 
    pred = {}
    real = {}
    objs = getpara(op, 'heaptime')
    for obj in objs:
      pred[obj] = []
      real[obj] = []
    for i in [1,2,3,4,5,6,8,9,10,11,12,14,15,16,17,18,19]:
    #for i in [4,5,8,9,10,12,18,19]:
      test_manual(objs, getpara(op, 'grid'), ['randtime_tpch_%d.csv' % i], pred, real)
    for obj in objs:
      print obj, metric(pred[obj], real[obj]), len(pred[obj]), min(real[obj]), max(real[obj]), sum(real[obj])/len(real[obj])
  if action == "testsingle": 
    test_single()
  jvm.stop()

if __name__ == "__main__":
  main(sys.argv)
