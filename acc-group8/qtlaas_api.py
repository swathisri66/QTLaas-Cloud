from flask import Flask, request, abort, jsonify, redirect
from keystoneauth1.identity import v3
from keystoneauth1 import session
import heatclient
from heatclient import client as heat_client
from heatclient.common import template_utils
import sys
from getpass import getpass
import time
import os
import subprocess

keystone_settings = {
	'auth_url': 'https://uppmax.cloud.snic.se:5000/v3',
	'project_id': '2344cddf33a1412b846290a9fb90b762',
	'project_name': 'SNIC 2018/10-30',
	'user_domain_name': 'snic',
	'username': sys.argv[1],
	'password': getpass('Password: ')
}

keystone_auth = v3.Password(**keystone_settings)
keystone_session = session.Session(auth=keystone_auth)
auth_url = 'https://uppmax.cloud.snic.se:5000/v3'
kwargs = {
    'auth_url': auth_url,
    'session': keystone_session,
    'auth': keystone_auth,
    'service_type': 'orchestration'
}

hc = heat_client.Client('1', **kwargs)

app = Flask(__name__)

stack_active = False

def write_hosts_to_master_and_worker(resp):
    with open('upload_hosts.sh', 'w') as f:
        f.write('scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q /etc/hosts ubuntu@' + resp['worker_ip']['output']['output_value'] + ':/etc/hosts' +'\n')
        f.write('scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q /etc/hosts ubuntu@' + resp['spark_private_ip']['output']['output_value'] + ':/etc/hosts' +'\n')
        f.write('sleep 5' +'\n')
        f.write('ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ubuntu@' + resp['spark_private_ip']['output']['output_value'] +" 'sudo nohup /usr/local/spark-2.2.2-bin-hadoop2.6/sbin/start-master.sh &'" + '\n')
        f.write('sleep 2' + '\n')
        f.write('ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ubuntu@' + resp['worker_ip']['output']['output_value'] + " 'sudo nohup /usr/local/spark-2.2.2-bin-hadoop2.6/sbin/start-slave.sh spark://" + resp['spark_name']['output']['output_value'] + ":7077 &'" + '\n')
        f.close()

    time.sleep(5)
    path = 'ubuntu@' + resp['ansible_ip']['output']['output_value'] + ':~/'
    filename = 'upload_hosts.sh'
    sts = 1
    while sts != 0:
        p = subprocess.Popen('scp -v -v -v -i group8key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ' + filename + ' ' + path, shell=True)
        sts = p.wait()
        print(sts)
    os.system('ssh -i group8key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q  ubuntu@' + resp['ansible_ip']['output']['output_value'] +  " 'sh ~/upload_hosts.sh'")

    command = ' "jupyter notebook list | ' + "grep -Po '=(.*?) ' | " + "sed 's/=//g'" + '"'
    with open('get_token.sh', 'w') as f:
        f.write('ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ubuntu@' + resp['spark_ip']['output']['output_value'] + command + '\n')

    time.sleep(5)
    path = 'ubuntu@' + resp['ansible_ip']['output']['output_value'] + ':~/'
    filename = 'get_token.sh'
    sts = 1
    while sts != 0:
        p = subprocess.Popen('scp -v -v -v -i group8key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ' + filename + ' ' + path, shell=True)
        sts = p.wait()
        print(sts)

 

def write_to_hosts_file(resp):
    with open('hosts', 'w') as f:
        f.write(resp['ansible_private_ip']['output']['output_value'] + ' ' + resp['ansible_name']['output']['output_value'] + '\n')
        f.write(resp['spark_private_ip']['output']['output_value'] + ' ' + resp['spark_name']['output']['output_value'] + '\n')
        f.write(resp['worker_ip']['output']['output_value'] + ' ' + resp['worker_name']['output']['output_value'] + '\n')
        f.close()

    time.sleep(10)
    path = 'ubuntu@' + resp['ansible_ip']['output']['output_value'] + ':/etc/hosts'
    filename = 'hosts'
    sts = 1
    while sts != 0:
        p = subprocess.Popen('scp -v -v -v -i group8key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ' + filename + ' ' + path, shell=True)
        sts = p.wait()
        print(sts)

def write_to_ansible_hosts_file(resp):
    with open('ansible', 'w') as f:
        f.write(resp['ansible_name']['output']['output_value'] + ' ansible_ssh_host=' + resp['ansible_private_ip']['output']['output_value'] + '\n')
        f.write(resp['spark_name']['output']['output_value'] + ' ansible_ssh_host=' + resp['spark_private_ip']['output']['output_value'] + '\n')
        f.write(resp['worker_name']['output']['output_value'] + ' ansible_ssh_host=' + resp['worker_ip']['output']['output_value'] + '\n')
        f.write('[configNode]' + '\n')
        f.write(resp['ansible_name']['output']['output_value'] + ' ansible_connection=local ansible_user=ubuntu' + '\n')

        f.write('[sparkmaster]' + '\n')
        f.write(resp['spark_name']['output']['output_value'] + ' ansible_connection=ssh ansible_user=ubuntu' + '\n')

        f.write('[sparkworker]' + '\n')
        f.write(resp['worker_name']['output']['output_value'] + ' ansible_connection=ssh ansible_user=ubuntu' + '\n')
        f.close()

    time.sleep(5)
    path = 'ubuntu@' + resp['ansible_ip']['output']['output_value'] + ':/etc/ansible/hosts'
    filename = 'ansible'
    sts = 1
    while sts != 0:
	    p = subprocess.Popen('scp -v -v -v -i group8key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ' + filename + ' ' + path, shell=True)
	    sts = p.wait()
	    print(sts)

@app.route('/qtlaas/start/<string:stack_name>', methods=['GET'])
def start(stack_name):
    global stack_active
    global stackname
    stackname = stack_name

    template_name = 'Heat_template_start_instance.yml'
    files, template = template_utils.process_template_path(template_name)

    if stack_active:
        abort(400, 'A stack is already active')

    try:
        hc.stacks.create(stack_name=stack_name, template=template, files=files)
        time.sleep(10)

        stacks = hc.stacks.list(filters={'stack_name': stack_name})
        stack = next(stacks)
        
        stack_status = stack.status
        while stack_status == 'IN_PROGRESS':
            stacks = hc.stacks.list(filters={'stack_name': stack_name})
            stack = next(stacks)
            stack_output = hc.stacks.output_list(stack.id)
            print 'Build in progress, sleep for 5 seconds...'
            time.sleep(5)
            stack_status = stack.status

        stack_active = True
        result = {}
        for line in stack_output['outputs']:
            output_value = line['output_key']
            result[output_value] = hc.stacks.output_show(stack.id, output_value)

        write_to_hosts_file(result)
        write_to_ansible_hosts_file(result)
        write_hosts_to_master_and_worker(result)

        token = subprocess.check_output('ssh -i group8key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q ubuntu@' + result['ansible_ip']['output']['output_value'] +  " 'sh ~/get_token.sh'")
        result['token'] = token

        return jsonify(result)
        # redirect('http://IP.TO.SPARK.MASTER:60060/', 302, jsonify(result))
    except heatclient.exc.HTTPConflict as e:
        abort(400, 'Stack already exists : %s %s' % (e.error, stack_name))
    except heatclient.exc.HTTPBadRequest as e:
        abort(400, 'Bad request : %s' % e.error)

@app.route('/qtlaas/stop')
def stop():
    global stack_active

    if not stack_active:
        abort(400, 'No stack is active')

    stacks = hc.stacks.list(filters={'stack_name': stackname})
    stack_id = next(stacks).id
    hc.stacks.delete(stack_id)
    stack_active = False
    return 'Deletion complete'


if __name__=='__main__':
    app.run(host='0.0.0.0', debug=True)
