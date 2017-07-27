#!/usr/bin/env python

import json
import csv
import copy
import sys
import subprocess
import time

def main(argv):
  while True:
    r = int(subprocess.check_output('ps aux | grep scheduler | grep -v grep | wc -l', shell=True))
    if r == 0:
      subprocess.call('./get_instances.py kill all', shell=True)
      break
    time.sleep(5)

if __name__ == "__main__":
  main(sys.argv)
