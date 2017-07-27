#!/usr/bin/env python
import json
import time
import random
import os.path
import subprocess
import requests
import re

if not os.path.isfile("/home/ec2-user/.ssh/id_rsa"):
  args = ["ssh-keygen", "-f", "/home/ec2-user/.ssh/id_rsa", "-N", ""]
  subprocess.call(args)
  args = "cat /home/ec2-user/.ssh/id_rsa.pub >> /home/ec2-user/.ssh/authorized_keys"
  subprocess.call(args, shell=True)
  args = "echo \"Host *\" >> /home/ec2-user/.ssh/config"
  subprocess.call(args, shell=True)
  args = "echo \"    StrictHostKeyChecking no\" >> /home/ec2-user/.ssh/config"
  subprocess.call(args, shell=True)
  args = "chmod 600 /home/ec2-user/.ssh/config"
  subprocess.call(args, shell=True)


