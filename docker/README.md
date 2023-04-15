# Spark and Docker Swarm setup

## 1. VM setup general setup

To run this set-up Ubuntu 22.04 was used. The cloud-init.yaml file can be used to install the needed packages for running a master or worker VM. If creating a VM from cloud-init is not available
the file can be used to run each command in the commandline. 

## 2. Master Node setup

### 1. Starting the swarm cluster

Run the docker swarm init command to start the cluster:
```bash
docker swarm init --advertise-addr <MASTER_LOCAL_IP>
```
This will create a join token that is used to join workers to the swarm that looks like this: 
```bash
docker swarm join --token <TOKEN> <MASTER_IP_WITH_PORT>
```
The token can also be accessed with:
```bash
docker swarm join-token worker
```
To run the cluster in Openstack the MTU of the docker networks needs to be adjusted. This is done by adding a daemon config file to /etc/docker/daemon.json.

This needs to be added to that file: 
```yaml
{
  mtu: 1450
}
```
The docker damon then needs to reloaded and restarted: 
```bash
systemctl daemon-reload
systemctl restart docker
```

### 2. Creating a registry 

A registry can be added to the swarm where the nodes can pull images from, using dokcer hub also works. This is how to deploy a registry:

This will deploy a registry container on the manager node: 
```bash
docker service create --name registry --constraint node.role==manager --publish published=5000,target=5000 registry:2
```
For docker to use this registry it needs to be added to the daemon config file /etc/docker/daemon.json.

With the MTU and registry settings the file should look like this: 
```yaml
{
  "insecure-registries" : ["http://LOCAL_MASTER_IP:5000"],
  mtu: 1450
}
```
The docker daemon then needs to reloaded and restarted: 
```bash
systemctl daemon-reload
systemctl restart docker
```
## 3. Worker Node setup

### 1. Join the docker swarm cluster with the join token.  
```bash
docker swarm join --token <TOKEN> <MASTER_IP_WITH_PORT>
```

### 2. Docker daemon configuration 

Add the daemon configuration to daemon config file to /etc/docker/daemon

With the MTU and registry settings the file should look like this: 
```yaml
{
  "insecure-registries" : ["http://LOCAL_MASTER_IP:5000"],
  mtu: 1450
}
```
The docker daemon then needs to reloaded and restarted: 
```bash
systemctl daemon-reload
systemctl restart docker
```
## 4. Build DockerFile using docker-compose
Going back to the master node.

To build the docker image and push it to the registry use these commands:
```bash
docker-compose build
docker-compose push
```

## 5. Push stack to the swarm

This command will push a stack to the swarm which contains one master container, two worker containers and an overlay network. 
```bash
docker stack deploy --with-registry-auth --compose-file docker-compose.yml sparkswarm
```

It should now be possible to reach the HDFS Namenode and Spark master web-UIs on port 8080 and 9870.

## 6. Add data to HDFS

Do these steps after setting up the Worker Nodes to have access to all the storage in the cluster. 

### 1. Copy files to the master container.

First find the docker container id with:
```bash
docker ps
```
Then use the ID to copy a file to the container: 
```bash
docker cp <Filename> <Container-ID>:<Filename>
```
### 2. Transfer files to HDFS
Start terminal in Container:
```bash
docker exec -it <Container-ID>  /bin/bash
```
In the container use DFS to create user folders:
```bash
/usr/local/hadoop/bin/hdfs dfs -mkdir /user
/usr/local/hadoop/bin/hdfs dfs -mkdir /user/<Username>
```
Then copy the file to HDFS:
```bash
/usr/local/hadoop/bin/hdfs dfs -put <Filename>
```

## 7.Run python file in the cluster
To run a python file in the cluster copy the python code to the docker master container using the "docker cp" command used before.

The "docker cp" command can also be used to get the output from the spark-cluster or nano can be used to view the file in the terminal.