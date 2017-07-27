#!/usr/bin/env python

import json
import copy
import myria_utils
import requests
import time
import os
import subprocess

ingest = {
  "relationKey" : {
    "userName" : "public",
    "programName" : "adhoc",
    "relationName" : "smallTable"
  },
  "schema" : {
    "columnTypes" : ["LONG_TYPE", "LONG_TYPE"],
    "columnNames" : ["col1", "col2"]
  },
  "source" : {
    "dataType" : "URI",
    "uri" : "/path/to/your/myria/jsonQueries/getting_started/smallTable"
  },
  "overwrite" : False,
  "delimiter": "|"
}

schema = {
    'public:adhoc:nation' : [
        ('n_nationkey', 'LONG_TYPE'),  # 25 nations
        ('n_name', 'STRING_TYPE'),
        ('n_regionkey', 'LONG_TYPE'),
        ('n_comment', 'STRING_TYPE')],
    'public:adhoc:lineitem' : [
      ('l_orderkey', 'LONG_TYPE'),
      ('l_partkey', 'LONG_TYPE'),
      ('l_suppkey', 'LONG_TYPE'),
      ('l_linenumber', 'LONG_TYPE'),
      ('l_quantity', 'LONG_TYPE'),
      ('l_extendedprice', 'DOUBLE_TYPE'),
      ('l_discount', 'DOUBLE_TYPE'),
      ('l_tax', 'DOUBLE_TYPE'),     
      ('l_returnflag', 'STRING_TYPE'), # only needs to be size 1
      ('l_linestatus', 'STRING_TYPE'), # only needs to be size 1
      ('l_shipdate', 'STRING_TYPE'), # DATE
      ('l_commitdate', 'STRING_TYPE'), # DATE
      ('l_receiptdate', 'STRING_TYPE'), # DATE
      ('l_shipinstruct', 'STRING_TYPE'),
      ('l_shipmode', 'STRING_TYPE'),
      ('l_comment', 'STRING_TYPE')],
    'public:adhoc:part' : [
      ('p_partkey', 'LONG_TYPE'), 
      ('p_name', 'STRING_TYPE'),
      ('p_mfgr', 'STRING_TYPE'),
      ('p_brand', 'STRING_TYPE'),
      ('p_type', 'STRING_TYPE'),
      ('p_size', 'LONG_TYPE'),
      ('p_container', 'STRING_TYPE'),
      ('p_retailprice', 'DOUBLE_TYPE'),
      ('p_comment', 'STRING_TYPE')],
    'public:adhoc:supplier' : [
      ('s_suppkey', 'LONG_TYPE'),
      ('s_name', 'STRING_TYPE'),
      ('s_address', 'STRING_TYPE'),
      ('s_nationkey', 'LONG_TYPE'),
      ('s_phone','STRING_TYPE'),
      ('s_acctbal', 'DOUBLE_TYPE'),
      ('s_comment', 'STRING_TYPE')],
    'public:adhoc:partsupp' : [
      ('ps_partkey',   'LONG_TYPE'),
      ('ps_suppkey', 'LONG_TYPE'),
      ('ps_availqty', 'LONG_TYPE'),
      ('ps_supplycost', 'DOUBLE_TYPE'),
      ('ps_comment', 'STRING_TYPE')],
    'public:adhoc:customer' : [
        ('c_custkey', 'LONG_TYPE'),
        ('c_name', 'STRING_TYPE'),
        ('c_address', 'STRING_TYPE'),
        ('c_nationkey', 'LONG_TYPE'),
        ('c_phone', 'STRING_TYPE'),
        ('c_acctbal', 'DOUBLE_TYPE'),
        ('c_mktsegment', 'STRING_TYPE'),
        ('c_comment', 'STRING_TYPE')],
    'public:adhoc:orders' : [
        ('o_orderkey', 'LONG_TYPE'),
        ('o_custkey', 'LONG_TYPE'),
        ('o_orderstatus', 'STRING_TYPE'), # size 1
        ('o_totalprice', 'STRING_TYPE'),
        ('o_orderdate', 'STRING_TYPE'),
        ('o_orderpriority', 'STRING_TYPE'),
        ('o_clerk', 'STRING_TYPE'),
        ('o_shippriority', 'LONG_TYPE'),
        ('o_comment', 'STRING_TYPE')],
    'public:adhoc:region' : [
        ('r_regionkey', 'LONG_TYPE'), # 5 regions
        ('r_name', 'STRING_TYPE'),
        ('r_comment', 'STRING_TYPE')],
}
bucket = 'http://tpch-1g.s3.amazonaws.com/'

def download(path):
  for rel in schema:
    relname = rel.split(':')[2]
    if not os.path.isfile('%s/%s.1.csv' % (path, relname)):
      subprocess.call('wget %s%s.1.csv -P %s' % (bucket, relname, path), shell=True)
    if not os.path.isfile('%s/%s.2.csv' % (path, relname)):
      subprocess.call('wget %s%s.2.csv -P %s' % (bucket, relname, path), shell=True)
    print 'downloaded', rel, path

def ingesttpchdata(hostname, deploy):
  for rel in schema:
    names = []
    types = []
    for col in schema[rel]:
      names.append(col[0])
      types.append(col[1])
    relname = rel.split(':')[2]
    ingest_cnt = copy.deepcopy(ingest)
    ingest_cnt['relationKey']['relationName'] = relname
    ingest_cnt['source']['uri'] = bucket + relname + '.1.csv'
    ingest_cnt['schema']['columnNames'] = names
    ingest_cnt['schema']['columnTypes'] = types
    requests.get(myria_utils.url(hostname, deploy['workers'][0][3]) + "sysgc")
    r = requests.post(myria_utils.url(hostname, deploy['master_rest']) + "dataset", data=json.dumps(ingest_cnt), headers={'Content-Type':'application/json'})
    print 'ingested', rel, hostname, r.text
    ingest_cnt['relationKey']['relationName'] = relname + '2'
    ingest_cnt['source']['uri'] = bucket + relname + '.2.csv'
    requests.get(myria_utils.url(hostname, deploy['workers'][0][3]) + "sysgc")
    r = requests.post(myria_utils.url(hostname, deploy['master_rest']) + "dataset", data=json.dumps(ingest_cnt), headers={'Content-Type':'application/json'})
    print 'ingested', rel + '2', hostname, r.text

