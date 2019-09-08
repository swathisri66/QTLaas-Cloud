import os
import sys
import time
import logging
from os import system
import get_ansible_workers
from novaclient import client
from os import environ as env
from keystoneauth1 import loading
from keystoneauth1 import session
import glanceclient.v2.client as glclient

# Setting up logging parameters
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setting up cloud configuration parameters
flavor_name = "ACCHT18.normal"
private_net = "SNIC 2018/10-30 Internal IPv4 Network"
floating_ip_pool_name = None
floating_ip = None
image_name = "Ubuntu 16.04 LTS (Xenial Xerus) - latest"
keyname = "group12"
loader = loading.get_plugin_loader('password')

# Authorizing user from global variables
auth = loader.load_from_options(auth_url=env['OS_AUTH_URL'],
                                username=env['OS_USERNAME'],
                                password=env['OS_PASSWORD'],
                                project_name=env['OS_PROJECT_NAME'],
                                project_domain_name=env['OS_USER_DOMAIN_NAME'],
                                project_id=env['OS_PROJECT_ID'],
                                user_domain_name=env['OS_USER_DOMAIN_NAME'])

sess = session.Session(auth=auth)
nova = client.Client('2.1', session=sess)
glance = glclient.Client('2.1', session=sess)
logger.info("__ACC__: Successfully completed User Authorization.")
worker_name = "Group12_Worker"


# Simple function to print all Group12 relevant instances
def find_all_instances():
    relevant_instances = nova.servers.list(search_opts={"name": "Group12"})
    for instance in relevant_instances:
        ip = instance.networks[private_net][0]
        name = instance.name
        if worker_name in name:
            print("Worker Instance: ", name, "Has the IP: ", ip)
        else:
            print("Instance: ", name, "Has the IP: ", ip)


# Simple function to update the file /etc/ansible/hosts
def update_ansible_hosts_file(lines):
    try:
        f_ansible = open("/etc/ansible/hosts", "r+")
    except:
        logger.error("__ACC__:Something went wrong while trying to open the file /etc/ansible/hosts. Make sure you "
                     "have permissions to open this file and try again. ")
        return False
    old = f_ansible.read()
    f_ansible.seek(0)
    f_ansible.write(lines + old)
    f_ansible.close()
    return True


def run_linux_cmds(linux_cmds):
    for command in linux_cmds:
        try:
            system(command)
        except:
            logger.error("__ACC__:Something went wrong while attempting to run: " + linux_cmds +
                         "Skipping this instance")
            logger.error('__ACC__: Try to run the command manually.')
            continue


def save_linux_cmds(linux_cmds):
    f = open("linux_commands.txt", "w")
    for command in linux_cmds:
        f.write(command + "\n")
    f.close()
    return True


# Responsible for detecting new relevant workers in the cloud, updating the hosts files with the correct IPs and names,
# and copying the id_rsa.pub from the master to the new workers' authorized_keys file
# Return:
#          False -> if no new workers were detected or an error occured.
#          True -> if new workers were detected and successfully updated.
def find_new_workers():
    logger.info("__ACC__: Looking for new workers...")
    workers_instances = nova.servers.list(search_opts={"name": worker_name})
    cluster_workers = get_ansible_workers.return_workers()
    if len(workers_instances) == len(cluster_workers):
        logger.info("__ACC__: No new workers found in Openstack.")
        return False
    else:
        try:
            f = open("/etc/hosts", "a")
        except:
            logger.error("__ACC__:Something went wrong while trying to open the file /etc/hosts. Make sure you "
                         "have permissions to open this file and try again. ")
            return False
        try:
            f_ansible = open("/etc/ansible/hosts", "a")
        except:
            logger.error("__ACC__:Something went wrong while trying to open the file /etc/ansible/hosts. Make sure you "
                         "have permissions to open this file and try again. ")
            return False
        logger.info("__ACC__:New worker(s) successfully detected in Openstack.")
        lines = ""
        linux_cmds = []
        for index in range(len(workers_instances)):
            try:
                worker_instance = workers_instances[index]
                name = worker_instance.name.lower()
                string_compare = "spark"+name[name.find("worker"):]
                if string_compare in cluster_workers:
                    continue
                else:
                    ip = worker_instance.networks[private_net][0]
                    line = ip + " " + string_compare + "\n"
                    f.write(line)
                    ansible_line = string_compare + " ansible_connection=ssh ansible_user=ubuntu\n"
                    f_ansible.write(ansible_line)
                    ansible_start_line = string_compare + " ansible_ssh_host=" + ip + "\n"
                    lines += ansible_start_line
                    linux_cmd = 'ssh ubuntu@' + string_compare + \
                                ' "cat >> ~/.ssh/authorized_keys" < ~/.ssh/id_rsa.pub'
                    linux_cmds.append(linux_cmd)

            except:
                logger.error("__ACC__:Something went wrong while checking the instance: "+workers_instances[index] +
                             ". Skipping this instance.")
                continue
        f.close()
        f_ansible.close()
        # run_linux_cmds(linux_cmds)
        save_linux_cmds(linux_cmds)
        return update_ansible_hosts_file(lines)


# Generates the name of the new worker
# Return: string Group12_WorkerI... where I is the index of the new worker
def get_new_worker_name():
    indices = []
    workers_instances = nova.servers.list(search_opts={"name": worker_name})
    if workers_instances:
        for worker in workers_instances:
            name = worker.name
            print("Name:", name)
            index = name[name.find("Worker")+len("Worker"):]
            indices.append(int(index))
        indices.sort()
        instance_name = worker_name + str(indices[-1]+1)
    else:
        instance_name = worker_name + str(1)
    return instance_name


def create_new_instance(image_name="Ubuntu 16.04 LTS (Xenial Xerus) - latest", instance_name=None, master=False):
    image = nova.images.find(name=image_name)
    flavor = nova.flavors.find(name=flavor_name)
    keyname = "group12"
    if private_net is not None:
        net = nova.networks.find(label=private_net)
        nics = [{'net-id': net.id}]
    else:
        sys.exit("private-net not defined.")
    if not master:
        cloud_cfg_filename = '/cloud-cfg.txt'
    else:
        cloud_cfg_filename = '/cloud-cfg-master.txt'

    cfg_file_path = os.getcwd() + cloud_cfg_filename
    if os.path.isfile(cfg_file_path):
        userdata = open(cfg_file_path)
    else:
        sys.exit(cloud_cfg_filename + " is not in current working directory")
    secgroups = ['default']
    logger.info("__ACC__:Creating new instance...")
    if instance_name is None:
        instance_name = get_new_worker_name()

    instance = nova.servers.create(name=instance_name, image=image, flavor=flavor, userdata=userdata, nics=nics,
                                   key_name=keyname, security_groups=secgroups)
    inst_status = instance.status
    logger.info("__ACC__:Waiting 10 seconds...")
    time.sleep(10)
    while inst_status == 'BUILD':
        logger.info("__ACC__:Instance" + instance.name + " is in " + inst_status +
                    "state, sleeping for 5 seconds more...")
        time.sleep(5)
        instance = nova.servers.get(instance.id)
        inst_status = instance.status

    logger.info("__ACC__:Instance: " + instance.name + " is in " + inst_status + "state")
    if inst_status == "ACTIVE":
        return True
    else:
        return False


def create_worker_snapshot():
    worker_image_name = "Group12_WorkerBase_Snapshot"
    found = False
    attempt = 1
    while not found:
        try:
            # Find image from snapshot in Openstack
            nova.images.find(name=worker_image_name)
            found = True
            logger.info("__ACC__:Image was found.")
        except:
            # No image from snapshot was found thus attempt to create snapshot from Group12_Worker1
            logger.info("__ACC__:No Image was found with name Worker_Base_Snapshot...")
            logger.info("__ACC__:Looking for worker to create snapshot from...")
            try:
                # Attempts to find instance Group12_Worker1
                base_worker = nova.servers.list(search_opts={"name": worker_name + str(1)})
                # Found instance Group12_Worker1 -> Attempts to create snapshot from it
                glance.images.create(name=worker_image_name, image=base_worker)
            except:
                # Since instance doesn't exist -> Attempts to create new instance Group12_Worker1
                # Repeat the loop until snapshot is created and/or image is found
                logger.info("__ACC__:No worker was found in Openstack.")
                logger.info("__ACC__: Attempt Number " + str(attempt) + " to create a new instance.")
                if attempt < 6:
                    if create_new_instance():
                        return True
                    attempt += 1
                else:
                    logger.error("__ACC__: 5 failed attempts to create a new working. Quitting...")
                    return False
    return create_new_instance(image_name=worker_image_name)


def delete_worker(delete_worker_name=None):
    if delete_worker_name:
        worker_instance = nova.servers.list(search_opts={"name": delete_worker_name})
        if len(worker_instance) == 0:
            logger.info("__ACC__:No workers was found in Openstack to delete.")
            return False
        else:
            logger.info("__ACC__:Found the instance. Attempting to delete it...")
            worker_instance = worker_instance[0]
    else:
        delete_worker_name = "Group12_Worker"
        workers = {}
        workers_list = nova.servers.list(search_opts={"name": delete_worker_name})
        if len(workers_list) == 0:
            logger.info("__ACC__:No workers was found in Openstack to delete.")
            return False
        for worker in workers_list:
            worker_name = worker.name.lower()
            worker_index = worker_name[worker_name.find("worker") + len("worker"):]
            workers[worker_index] = worker
        worker_instance = workers[sorted(workers.keys())[-1]]
    try:
        ip = worker_instance.networks[private_net][0]
        nova.servers.delete(worker_instance)
        logger.info("__ACC__: Successfully deleted the instance: ", worker_instance)
        return ip
    except:
        logger.error("__ACC__: Failed to remove a worker. Quitting...")
        return False


def edit_file(file_name, compare_line):
    f = open(file_name, "r")
    lines = f.readlines()
    removed_lines = []
    f.close()
    f = open(file_name, "w")
    for line in lines:
        if compare_line in line.strip().split():
            logger.info("__ACC__: Removed the host line: ", line.strip(), "from the file: ", file_name)
            removed_lines.append(line)
            continue
        f.write(line)
    f.close()
    return removed_lines


def remove_cluster_worker():
    ip = delete_worker()
    if not ip:
        logger.error("__ACC__: Unable to remove worker from the cluster...")
        return False
    try:
        removed_lines = edit_file(file_name="/etc/hosts", compare_line=ip)
    except:
        logger.error("__ACC__: Failed to edit the file /etc/hosts. Please remove manually the hosts with IP: ", ip)
        return False
    for line in removed_lines:
        try:
            removed_host = line.strip().split()[1]
        except:
            logger.error("__ACC__: The file /etc/hosts with the IP", ip, "is not found. Check syntax..")
            return False
        try:
            edit_file(file_name="/etc/ansible/hosts",compare_line=removed_host)

        except:
            logger.error("__ACC__: Failed to edit the file /etc/ansible/hosts. "
                         "Please remove manually the hosts with hostname: ", removed_host)
            return False
    return True


def edit_master_file(file_name, lines):
    try:
        f = open(file_name, "a")
    except:
        logger.error("__ACC__:Something went wrong while trying to open the file:", file_name," . Make sure you "
                     "have permissions to open this file and try again. ")
        return False
    f.write(lines)
    f.close()
    return True


def setup_master_node(master_name=None):

    if master_name is None:
        master_name = "Group12_Master"

    master_instance = nova.servers.list(search_opts={"name": master_name})[0]
    ip = master_instance.networks[private_net][0]

    # Create lines to update /etc/hosts with
    lines = "\n"
    line = ip + " ansible-node\n"
    lines += line
    line = ip + " sparkmaster\n"
    lines += line

    # Update /etc/hosts with above lines
    if edit_master_file(file_name="/etc/hosts", lines=lines):
        logger.info("__ACC__: Successfully updated the file /etc/hosts.")
    else:
        logger.error("__ACC__:Something went wrong while trying to open the file /etc/ansible/hosts. Make sure you "
                     "have permissions to open this file and try again. ")
        return False

    # Create lines to append in /etc/ansible/hosts
    lines = "\n[configNode]\n"
    lines += "ansible-node ansible_connection=local ansible_user=ubuntu\n\n"
    lines += "[sparkmaster]\n"
    lines += "sparkmaster ansible_connection=local ansible_user=ubuntu\n\n"
    lines += "[sparkworker]\n"
    # Append above lines /etc/ansible/hosts

    if edit_master_file(file_name="/etc/ansible/hosts", lines=lines):
        logger.info("__ACC__: Successfully updated the file /etc/ansible/hosts.")
    else:
        logger.error("__ACC__:Something went wrong while trying to open the file /etc/hosts. Make sure you "
                     "have permissions to open this file and try again. ")
        return False

    # Create lines to update /etc/ansible/hosts with
    lines = "\nansible-node ansible_ssh_host=" + ip + "\n"
    lines += "sparkmaster ansible_ssh_host=" + ip + "\n"

    # Update /etc/hosts with above lines
    if update_ansible_hosts_file(lines):
        logger.info("__ACC__: Successfully updated the file /etc/hosts.")
    else:
        logger.error("__ACC__:Something went wrong while trying to open the file /etc/hosts. Make sure you "
                     "have permissions to open this file and try again. ")
        return False
