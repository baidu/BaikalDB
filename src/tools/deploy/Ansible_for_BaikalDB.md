## 一、概述

本文档介绍如何使用ansible部署一个BaikalDB集群。

Ansible 一种集成 IT 系统的配置管理、应用部署、执行特定任务的开源平台。详细介绍：https://www.ansible.com/

## 二、准备工作

### 1. 机器清单

Ansible需要安装在一台发布机上，通过建立信任关系，对多台部署机进行远程部署。

| 部署组件 | 主机IP                                  |
| -------- | --------------------------------------- |
| ansible  | xx.xx.xx.153                            |
| Meta     | xx.xx.xx.99, xx.xx.xx.136, xx.xx.xx.156 |
| Store    | xx.xx.xx.99, xx.xx.xx.136, xx.xx.xx.156 |
| db       | xx.xx.xx.99, xx.xx.xx.136, xx.xx.xx.156 |

### 2. 安装ansible

```
$ yum install epel-release -y
$ yum install ansible –y
```

### 3. 主机清单

主机清单保存了我们需要管理的主机列表。我们需要根据准备的机器来修改所要部署机器的IP。

以下示例中，meta代表主机列表，ansible工具会对所选主机列表的所有主机进行部署。示例如下：

```
$ vim /etc/ansible/hosts
[meta]
xx.xx.xx.99
xx.xx.xx.136
xx.xx.xx.156

[store]
xx.xx.xx.99
xx.xx.xx.136
xx.xx.xx.156

[db]
xx.xx.xx.99
xx.xx.xx.136
xx.xx.xx.156
```

### 4. 建立信任关系

部署ansible的机器需要与部署BaikalDB的机器建立信任关系。

ansible是基于ssh协议实现的，建立信任关系步骤与ssh类似：

```
# ansible机器 xx.xx.xx.153
$ ssh-keygen 	# 一路回车即可
$ cat ~/.ssh/id_rsa.pub # 复制公钥id_rsa.pub的内容

# BaikalDB机器 xx.xx.xx.99, xx.xx.xx.136, xx.xx.xx.156
$ vim ~/.ssh/authorized_keys	# 将所复制公钥的内容粘贴到authorized_keys中
$ chmod 700 /home/work  # 这里设置work用户的权限
$ chmod 700 ~/.ssh
$ chmod 600 ~/.ssh/authorized_keys

# 免密关系配置完成，可以在ansible机器使用ssh登录目标机器
# ssh端口默认为22，可以在/etc/ssh/sshd_config中查看端口号
# 上面步骤修改work用户的权限，所以这里使用work用户登录目标机器
$ ssh -p 22 work@xx.xx.xx.99
$ ssh -p 22 work@xx.xx.xx.136
$ ssh -p 22 work@xx.xx.xx.156

# 建立完信任关系可以使用ansible ping目标机器
$ ansible store -m ping
xx.xx.xx.136 | SUCCESS => {
    "ansible_facts": {
        "discovered_interpreter_python": "/usr/bin/python"
    }, 
    "changed": false, 
    "ping": "pong"
}
xx.xx.xx.99 | SUCCESS => {
    "ansible_facts": {
        "discovered_interpreter_python": "/usr/bin/python"
    }, 
    "changed": false, 
    "ping": "pong"
}
xx.xx.xx.156 | SUCCESS => {
    "ansible_facts": {
        "discovered_interpreter_python": "/usr/bin/python"
    }, 
    "changed": false, 
    "ping": "pong"
}
```

### 5.创建playbook一键部署

首先构建一个如下所示的BaikalDB目录，二进制编译与conf配置参照https://github.com/baidu/BaikalDB/wiki/Installation-EN。

![image-20201211164153805](https://github.com/lvxinup/Ansible_for_BaikalDB/raw/main/image-20201211164153805.png)

之后创建三个playbook文件以及部署脚本，内容如下：

```
$ vim meta.yml
---

- hosts: meta
  remote_user: work
  tasks:
    - name: mkdir BaikalDB
      command: mkdir BaikalDB
      args:
        chdir: /home/work
    - name: cp script to remote_user
      copy: src=/etc/ansible/BaikalDB/script dest=/home/work/BaikalDB backup=yes
      # src为源路径，/etc/ansible/BaikalDB 修改为部署机BaikalDB 的存放路径
      # dest为目标路径，/home/work/software/ansible_BaikalDB 修改为目标机器的部署路径
    - name: cp meta to remote_user
      copy: src=/etc/ansible/BaikalDB/meta dest=/home/work/BaikalDB backup=yes
    - name: mkdir log
      command: mkdir log
      args:
        chdir: /home/work/BaikalDB/meta      
    - name: chmod +x to meta
      command: chmod +x baikalMeta
      args:
        chdir: /home/work/BaikalDB/meta/bin
    - name: start meta
      shell: nohup bin/baikalMeta >/dev/null 2>&1 &
      args:
        chdir: /home/work/BaikalDB/meta
        
$ vim store.yml
---

- hosts: store
  remote_user: work
  tasks:
    - name: cp store to remote_user
      copy: src=/etc/ansible/BaikalDB/store dest=/home/work/BaikalDB backup=yes
    - name: mkdir log
      command: mkdir log
      args:
        chdir: /home/work/BaikalDB/store      
    - name: chmod +x to store
      command: chmod +x baikalStore
      args:
        chdir: /home/work/BaikalDB/store/bin
    - name: start store
      shell: nohup bin/baikalStore >/dev/null 2>&1 &
      args:
        chdir: /home/work/BaikalDB/store
        
$ vim db.yml
---

- hosts: db
  remote_user: work
  tasks:
    - name: cp db to remote_user
      copy: src=/etc/ansible/BaikalDB/db dest=/home/work/BaikalDB backup=yes
    - name: mkdir log
      command: mkdir log
      args:
        chdir: /home/work/BaikalDB/db      
    - name: chmod +x to db
      command: chmod +x baikaldb
      args:
        chdir: /home/work/BaikalDB/db/bin
    - name: start db
      shell: nohup bin/baikaldb >/dev/null 2>&1 &
      args:
        chdir: /home/work/BaikalDB/db
        
$ vim start.sh
#!/bin/bash

cd /etc/ansible
ansible-playbook meta.yml
sleep 3

line=`expr $(awk '/\[meta\]/{print NR}' /etc/ansible/hosts) + 1`
meta=$(cat -n /etc/ansible/hosts | sed -n ''$line'p' | awk '{print $2}')
leader_info=$(curl -d '{"op_type" : "GetLeader","region_id" : 0}' http://$meta:8010/MetaService/raft_control)
leader_ip=$(echo $leader_info | grep -Eo '"leader":"[0-9.]+*' |grep -Eo "[0-9.]+")
port=8010
echo "init meta"
sh BaikalDB/script/init_meta_server.sh $leader_ip:$port
sh BaikalDB/script/create_namespace.sh $leader_ip:$port
sh BaikalDB/script/create_database.sh $leader_ip:$port
sh BaikalDB/script/create_user.sh $leader_ip:$port

ansible-playbook store.yml

echo "init db"
sh BaikalDB/script/create_internal_table.sh $leader_ip:$port

ansible-playbook db.yml

# 建议启动之前先运行ping
$ ansible meta -m ping
$ ansible store -m ping
$ ansible db -m ping

# 启动脚本一键部署
$ sh start.sh

# 部署完成之后可以登录数据库
$ mysql -hxx.xx.xx.xx.99 -P28282 -utest -ptest
```