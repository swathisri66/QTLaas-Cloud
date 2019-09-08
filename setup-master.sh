#!/usr/bin/env bash
sudo chmod +x ansible_install.sh
./ansible_install.sh
ssh-keygen -t rsa -b 4096
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
source SNIC.sh
python -c 'from qtlaas_automation import setup_master_node; setup_master_node(master_name="Group8_Master")'
sudo chmod +x setup_worker.sh
sudo ./setup_worker.sh
ansible-playbook -s spark_deployment.yml
