#!/usr/bin/env python

import subprocess
import json
import sys
import os
import ingest_tpch

RACODIST = '/home/jwang/project/raco'
TPCH = '/home/jwang/project/tpch-dbgen'
DATA = '/data'

for i in range(20):
  if os.path.isfile('%s/q%d.myl' % (TPCH, i)):
    subprocess.call('sed \'s/scan/load/g\' %s/q%d.myl > %s/tmp.myl' % (TPCH, i, TPCH), shell=True)
    for sf in [1,2]:
      for relname in ['nation','lineitem','part','supplier','partsupp','customer','orders','region']:
        relfile = '%s/%s.%d.csv' % (DATA, relname, sf)
        subprocess.call('sed \'s=\\x27%s\\x27="file://%s"=g\' %s/tmp.myl > %s/tmp.1.myl' % (relname, relfile, TPCH, TPCH), shell=True)
        subprocess.call('cp %s/tmp.1.myl %s/tmp.myl' % (TPCH, TPCH), shell=True)
        s = []
        for col in ingest_tpch.schema['public:adhoc:%s' % relname]:
          t = col[1].split('_')[0].lower()
          if t == 'long': t = 'int'
          if t == 'double': t = 'float'
          s.append('%s:%s' % (col[0], t))
        schema = 'csv(schema(%s),delimiter="|")' % ','.join(s)
        subprocess.call('sed \'s@%s.%d.csv");@%s.%d.csv",%s);@g\' %s/tmp.myl > %s/tmp.1.myl' % (relname, sf, relname, sf, schema, TPCH, TPCH), shell=True)
        subprocess.call('cp %s/tmp.1.myl %s/tmp.myl' % (TPCH, TPCH), shell=True)
      subprocess.call('%s/scripts/myrial -j %s/tmp.myl > %s/q%d.%d.json' % (RACODIST, TPCH, TPCH, i, sf), shell=True)
    print i
