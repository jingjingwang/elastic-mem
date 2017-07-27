#!/usr/bin/env python

import subprocess

def send_command(command, host, port, check_output = False):
  try:
    if check_output:
      r = subprocess.check_output("echo \"%s\" | nc %s %d" % (command, host, port), shell=True).strip()
      return r
    else:
      subprocess.call("echo \"%s\" | nc %s %d" % (command, host, port), shell=True)
      return ''
  except subprocess.CalledProcessError as e:
    return ''

def heap_state(host, port):
  ret = {}
  r = send_command('stats', host, port, '', True).split()
  i = 0
  while i < len(r):
    ret[r[i]] = int(r[i+1])
    i += 2
  return ret

def emptyheapstate(state):
  ret = {}
  for k in state:
    ret[k] = 0
  return ret

def get(host, port, args):
  return send_command(' '.join(args), host, port, True)

def send(host, port, args):
  send_command(' '.join(args), host, port, False)

def eden_max(host, port):
  return heap_state(host, port)["emax"]

def survivor_max(host, port):
  return heap_state(host, port)["smax"]

def old_max(host, port):
  return heap_state(host, port)["omax"]

def extend_eden_cap(host, port, v):
  send_command('emax=%s' % v, host, port)

def extend_survivor_cap(host, port, v):
  send_command('smax=%s' % v, host, port)

def extend_old_cap(host, port, v):
  send_command('omax=%s' % v, host, port)

def trigger_fullgc(host, port):
  send_command('fullgc', host, port)

def trigger_gc(host, port):
  send_command('gc', host, port)

def kill(host, port):
  send_command('kill', host, port)

def gcactive(host, port):
  r = send_command('gcactive', host, port, True)
  return r.startswith('1')

def blocked_no_space(host, port):
  return send_command('blocked', host, port, True) == '1'

def opstats(host, port):
  return getstats(host, port, 'opstats')

def lastgcopstats(host, port):
  return getstats(host, port, 'lastgcopstats')

def getstats(host, port, command):
  r = send_command(command, host, port, '', True).split()[1:]
  i = 0
  ret = {}
  while i < len(r):
    opname = r[i][:r[i].find('_')]
    opkey = r[i][r[i].find('_')+1:]
    if opname not in ret:
      ret[opname] = {}
    ret[opname][opkey] = int(r[i+1])
    i += 2
  return ret
