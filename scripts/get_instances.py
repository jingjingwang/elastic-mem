#!/usr/bin/env python

import subprocess
import json
from datetime import datetime
from pytz import timezone
from threading import Thread
import sys
from thread import start_new_thread, allocate_lock

KEYNAME = 'jwang'
AWS_ACCESS_KEY='AKIAJBCFOTGQANAC2SVQ'
AWS_SECRET_KEY='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'
LOCAL_SSH_AUTH_KEYS = '/home/jwang/.ssh/authorized_keys'
TPCH_QUERIES = '/home/jwang/project/tpch-dbgen'
FILES_TO_COPY = ['scheduler.py', 'jvm.py', 'myria_utils.py', 'qplan.py', 'models', 'get_instances.py', 'train.py', 'gendata.py', 'ingest_tpch.py']
MYRIA = '/home/jwang/myria/myria/'
JAVADIST = '/home/jwang/project/jdk8u/build/linux-x86_64-normal-server-release/jdk/'

class subprocess_thread(Thread):
  def __init__(self, args):
    Thread.__init__(self)
    self.args = args
  def run(self):
    subprocess.call(self.args, shell=True)

def filter_instance(conf):
  args = 'aws ec2 describe-instances --filter \"Name=key-name,Values=%s\" \"Name=instance-state-name,Values=running\"' % KEYNAME
  if conf['value'] is not None:
    args += ' \"Name=%s,Values=%s\"' % (conf['key'], conf['value'])
  while True:
    try:
      r = subprocess.check_output(args, shell=True, stderr=open('/dev/null', 'w'))
      r = json.loads(r)["Reservations"]
      if len(r) == 0: return []
      break
    except subprocess.CalledProcessError as err:
      print err
  ret = []
  for res in r:
    for node in res["Instances"]:
      if conf['return'] == 'insid': ret.append(node["InstanceId"]) 
      else: ret.append(node["PublicDnsName"])  
  return ret

def check_disk_usage(instances):
  for instance in instances:
    args = "ssh -i %s.pem ec2-user@%s df | grep \"data\"" % (KEYNAME, instance[1])
    r = subprocess.check_output(args, shell=True)
    print instance, r.strip()

def clean_disk_usage(instances):
  for instance in instances:
    args = "ssh -i %s.pem ec2-user@%s rm -f /data/*_*" % (KEYNAME, instance)
    r = subprocess.check_output(args, shell=True)
    args = "ssh -i %s.pem ec2-user@%s rm -f /data/tempdata*" % (KEYNAME, instance)
    r = subprocess.check_output(args, shell=True)

def preprocess_instances(instances):
  for instance in instances:
    prep_ins_thread(instance).start()


class prep_ins_thread(Thread):
  def __init__(self, ins):
    Thread.__init__(self)
    self.ins = ins
  def run(self):
    hostname = self.ins
    cmd = "ssh -i %s.pem ec2-user@%s ps -u ec2-user -o pid,command | grep -E \'java|python\' | grep -v grep | awk \"{print \\$1}\"" % (KEYNAME, hostname)
    pids = filter(None, subprocess.check_output(cmd, shell=True).split('\n'))
    for pid in pids:
      subprocess.call("ssh -i %s.pem ec2-user@%s kill -9 %s" % (KEYNAME, hostname, pid), shell=True)

    if subprocess.check_output("ssh -i %s.pem ec2-user@%s test -d /data && echo 1 || echo 0" % (KEYNAME, hostname), shell=True).strip() == "0":
      subprocess.call("ssh -i %s.pem ec2-user@%s sudo mkfs.ext4 -E nodiscard /dev/xvdca" % (KEYNAME, hostname), shell=True)
      subprocess.call("ssh -i %s.pem ec2-user@%s sudo mkdir -p /data" % (KEYNAME, hostname), shell=True)
      subprocess.call("ssh -i %s.pem ec2-user@%s sudo mount -o discard /dev/xvdca /data" % (KEYNAME, hostname), shell=True)
    subprocess.call("ssh -i %s.pem ec2-user@%s sudo chown -R ec2-user /data" % (KEYNAME, hostname), shell=True)

    args = "rsync --del -rae \"ssh -i %s.pem\" %s ec2-user@%s:/data/myria/" % (KEYNAME, MYRIA, hostname)
    subprocess_thread(args).start()
    args = "rsync --del -L -rae \"ssh -i %s.pem\" %s ec2-user@%s:/data/jdk/" % (KEYNAME, JAVADIST, hostname)
    subprocess_thread(args).start()
    subprocess.call("scp -i %s.pem -q  ec2-user@%s:/home/ec2-user" % (KEYNAME, hostname), shell=True)

    if subprocess.check_output(("ssh -i %s.pem ec2-user@%s test -f /home/ec2-user/.ssh/id_rsa && echo 1 || echo 0") % (KEYNAME, hostname), shell=True).strip() == "0":
      subprocess.call("ssh -i %s.pem ec2-user@%s ssh-keygen -f /home/ec2-user/.ssh/id_rsa -N \\\"\\\"" % (KEYNAME, hostname), shell=True)
      subprocess.call("ssh -i %s.pem ec2-user@%s \"cat /home/ec2-user/.ssh/id_rsa.pub >> /home/ec2-user/.ssh/authorized_keys\"" % (KEYNAME, hostname), shell=True)
    subprocess.call("ssh -i %s.pem ec2-user@%s \'echo \"Host *\" > /home/ec2-user/.ssh/config\'" % (KEYNAME, hostname), shell=True)
    subprocess.call("ssh -i %s.pem ec2-user@%s \'echo \"    StrictHostKeyChecking no\" >> /home/ec2-user/.ssh/config\'" % (KEYNAME, hostname), shell=True)
    subprocess.call("ssh -i %s.pem ec2-user@%s \'chmod 600 /home/ec2-user/.ssh/config\'" % (KEYNAME, hostname), shell=True)
    subprocess.call("ssh -i %s.pem ec2-user@%s \'sudo sh -c \"echo 1 > /proc/sys/vm/overcommit_memory\"\'" % (KEYNAME, hostname), shell=True)

def tagrole(role, tag, limit=None):
  ins1 = set(filter_instance({'key':'tag-value', 'value':tag, 'return':'insid'}))
  ins2 = set(filter_instance({'key':'tag-key', 'value':'role', 'return':'insid'}))
  instances = sorted(ins1 - ins2)
  if limit is not None: instances = instances[:limit]
  args = "aws ec2 create-tags --resources %s --tags Key=%s,Value=%s" % (' '.join(instances), 'role', role)
  subprocess_thread(args).start()

def tagnew(v, limit=None):
  insall = set(filter_instance({'key':'tag-key', 'value':None, 'return':'insid'}))
  instagged = set(filter_instance({'key':'tag-key', 'value':'group', 'return':'insid'}))
  instances = list(insall.difference(instagged))
  if limit is not None: instances = instances[:limit]
  args = "aws ec2 create-tags --resources %s --tags Key=%s,Value=%s" % (' '.join(instances), 'group', v)
  subprocess_thread(args).start()

def replace(old, new, limit=None):
  ins = filter_instance({'key':'tag-value', 'value':old, 'return':'insid'})
  if limit is not None: ins = ins[:limit]
  args = "aws ec2 create-tags --resources %s --tags Key=%s,Value=%s" % (' '.join(ins), 'group', new)
  subprocess_thread(args).start()

def setup_scheduler(i):
  subprocess.call('ssh -i %s.pem ec2-user@%s sudo yum install -y java-1.8.0-openjdk-devel' % (KEYNAME, i), shell=True)
  subprocess.call('ssh -i %s.pem ec2-user@%s sudo yum remove -y java-1.7.0-openjdk' % (KEYNAME, i), shell=True)
  subprocess.call('ssh -i %s.pem ec2-user@%s sudo yum groupinstall -y \"Development tools\"' % (KEYNAME, i), shell=True)
  subprocess.call('ssh -i %s.pem ec2-user@%s sudo pip install numpy' % (KEYNAME, i), shell=True) 
  subprocess.call('ssh -i %s.pem ec2-user@%s sudo pip install javabridge python-weka-wrapper pytz' % (KEYNAME, i), shell=True) 
  subprocess.call('ssh -i %s.pem ec2-user@%s \'echo \"export AWS_ACCESS_KEY=%s\" >> /home/ec2-user/.bashrc\'' % (KEYNAME, i, AWS_ACCESS_KEY), shell=True)
  subprocess.call('ssh -i %s.pem ec2-user@%s \'echo \"export AWS_SECRET_KEY=%s\" >> /home/ec2-user/.bashrc\'' % (KEYNAME, i, AWS_SECRET_KEY), shell=True)
  subprocess.call('ssh -i %s.pem ec2-user@%s \'echo \"alias java=/data/jdk/bin/java\" >> /home/ec2-user/.bashrc\'' % (KEYNAME, i), shell=True)
  subprocess.call('ssh -i %s.pem ec2-user@%s \'echo \"export JAVA_HOME=/data/jdk\" >> /home/ec2-user/.bashrc\'' % (KEYNAME, i), shell=True)
  subprocess.call('ssh -i %s.pem ec2-user@%s mkdir -p logs /home/ec2-user/tpch' % (KEYNAME, i), shell=True) 
  subprocess.call('ssh -i %s.pem ec2-user@%s \"cat /home/ec2-user/.ssh/id_rsa.pub\" >> %s' % (KEYNAME, i, LOCAL_SSH_AUTH_KEYS), shell=True)
  subprocess.call('scp -i %s.pem -q -r %s ec2-user@%s:/home/ec2-user' % (KEYNAME, ' '.join(FILES_TO_COPY), i), shell=True)
  subprocess.call('scp -i %s.pem -q %s/q*.?.json ec2-user@%s:/home/ec2-user/tpch' % (KEYNAME, TPCH_QUERIES, i), shell=True)

def main(argv):
  if len(argv) == 1:
    instances = filter_instance({'key':'tag-value', 'value':None, 'return':'dns'})
    for ins in instances:
      print ins
    return
  action = argv[1]
  #  if argv[1] == "scheduler" or argv[1] == "worker":
  #    a = filter_instance({'key':'tag-value', 'value':argv[2], 'return':'dns'})
  #    b = filter_instance({'key':'tag-value', 'value':argv[1], 'return':'dns'})
  #    for i in set(a).intersection(set(b)): print i
  if action == "filter":
    instances = filter_instance({'key':'tag-value', 'value':argv[2], 'return':'dns'})
    for ins in instances: print ins
  elif action == "init":
    instances = filter_instance({'key':'tag-value', 'value':argv[2], 'return':'dns'})
    preprocess_instances(instances)
    setup_scheduler(instances[0])
  elif action == "diskclean":
    instances = filter_instance({'key':'tag-value', 'value':argv[2], 'return':'dns'})
    clean_disk_usage(instances)
  elif action == "tagnew":
    tagnew(argv[2], int(argv[3]) if len(argv) >= 4 else None)
    #elif "tagscheduler" in argv:
    #  tagrole('scheduler', argv[2], 1)
    #elif "tagworker" in argv:
    #  tagrole('worker', argv[2])
  elif action == "replace":
    old = argv[2]
    new = argv[3]
    if len(argv) >= 5: replace(old, new, int(argv[4]))
    else: replace(old, new)
  elif "kill" in argv:
    if len(argv) >= 3 and argv[2] == 'all':  
      instances = filter_instance({'key':'tag-value', 'value':None, 'return':'insid'})
    else:
      instances = filter_instance({'key':'tag-value', 'value':argv[2], 'return':'insid'})
    subprocess_thread('aws ec2 terminate-instances --instance-ids %s' % ' '.join(instances)).start()

if __name__ == "__main__":
  main(sys.argv)

