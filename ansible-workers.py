def return_count():
    f = open("/etc/ansible/hosts", "r")
    flag = False
    count = 0
    for line in f:
        if line.strip() != "[sparkworker]":
            if flag:
                count += 1
            else:
                continue
        else:
            flag = True
    f.close()
    return count

def return_workers():
    workers = []
    f = open("/etc/ansible/hosts", "r")
    flag = False
    for line in f:
        if line.strip() != "[sparkworker]":
            if flag:
                line = line.split()
                if len(line) == 0:
                    continue
                workers.append(line[0])
            else:
                continue
        else:
            flag = True
    f.close()
    return workers
