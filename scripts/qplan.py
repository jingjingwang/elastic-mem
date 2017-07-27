#!/usr/bin/env python

import json
import copy

def short_to_schema(short_types):
  types = []
  names = []
  for i, t in enumerate(short_types.split('_')):
    if t == 'L': types.append('LONG_TYPE')
    elif t.startswith('S'): types.append('STRING_TYPE')
    names.append("col%d" % i)
  return {"columnTypes":types, "columnNames":names}

def join_op(opid, name, child1, child2, num_key, num_out, triggers=[]): 
  return{'opType':'SymmetricHashJoin', "opName":name, "argChild1":child1["opId"], "argChild2":child2["opId"], "opId":opid, \
         "argColumns1":range(num_key), "argColumns2":range(num_key), "argSelect1":range(num_out), "argSelect2":[], 'triggers':triggers}

def agg_op(opid, name, child, num_key, strlen, triggers=[]):
  r = {"opType":"Aggregate", "opName":name, "argChild":child["opId"], \
       "aggregators": [{"type": "CountAll"}], "argGroupFields":range(num_key), "opId":opid}
  if triggers is not None: 
    #r["opType"] = "MultiGroupByAggregateGC"
    r['triggers'] = triggers
  return r

def empty_op(opid, conf):
  return {"opType":"Empty", "opId":opid, "schema":conf["schema"]}

def select_op(opid, name, conf):
  return {"opName":name, "opType":"TupleSource", "opId":opid, "source":copy.deepcopy(conf["source"]), "reader":{"schema":conf["schema"], "readerType":"CSV"}}

def root_op(opid, child):
  return {"opType":"EmptySink", "argChild":child["opId"], "opId":opid}

def one_hash_plan(conf, triggers=[]):
  cols = len(conf['schema']['columnTypes'])
  select1 = select_op(0, 'select1', conf)
  select2 = empty_op(1, conf)
  join = join_op(2, 'join', select1, select2, 1, cols, triggers)
  root = root_op(3, join)
  fragments = [{'operators':[select1, select2, join, root]}]
  return {'fragments':fragments, 'logicalRa':'', 'rawQuery':'', 'language':''}

def genschema(count_long, count_str, strlen):
  types = []
  for i in range(count_long):
    types.append('L')
  for i in range(count_str):
    types.append('S' + str(strlen))
  return '_'.join(types)

def more_schema_feature(stats):
  strlen = 0 if stats['strsum'] == 0 else stats['strsum'] / stats['str']
  stats['nC'] = stats['long'] + stats['str']
  stats['sT'] = stats['long'] * 8 + (strlen / 4 * 8 + 45) * stats['str']
  stats['sH'] = stats['nT'] * stats['sT']
  stats['sH_delta'] = stats['nT_delta'] * stats['sT']

