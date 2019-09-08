#!/usr/bin/env bash
source SNIC.sh
python -c 'from qtlaas_automation import create_new_instance; create_new_instance(instance_name="Group8_Worker1")'
python -c 'from qtlaas_automation import find_new_workers; find_new_workers()'
eval `ssh-agent -s`
ssh-add group8.pem
python run_linux_cmds.py
