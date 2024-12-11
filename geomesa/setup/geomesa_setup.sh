#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

ZOOKEEPER_VERSION="3.8.4"
ZOOKEEPER_HOME="$HOME/apache-zookeeper-${ZOOKEEPER_VERSION}-bin"
ZOOKEEPER_CONF_FILE="$HOME/apache-zookeeper-${ZOOKEEPER_VERSION}-bin/conf/zoo.cfg"
ZOOKEEPER_CONF="tickTime=2000
dataDir=/var/lib/zookeeper
initLimit=10
syncLimit=5
clientPort=2181
clientPortAddress=0.0.0.0"

HDFS_VERSION="3.3.6"
HDFS_HOME="$HOME/hadoop-${HDFS_VERSION}"
HDFS_CONF_DIR="$HDFS_HOME/etc/hadoop"

ACCUMULO_VERSION="2.1.3"
ACCUMULO_HOME="$HOME/accumulo-${ACCUMULO_VERSION}"
ACCUMULO_CONF_DIR="$ACCUMULO_HOME/conf"

GEOMESA_TAG="5.1.0"
GEOMESA_VERSION="2.12-${GEOMESA_TAG}" # 2.12 is the scala build version
GEOMESA_HOME="$HOME/geomesa-accumulo_${GEOMESA_VERSION}"

# data_path="Flight_Points_Actual_20220601_20220630.csv"
flightpoints_resource_id="15nXnXbJrYqKow4ckEtbmPc2W-tyZLI7Q"
flighttrips_resource_id="1JVIm0JPjWKYqO9xCsb8ELDtSrdhhgppQ"
counties_resource_id="1XVXtuIhcgGk7OZhiO7Q56Lv-tgcb9swz"
districts_resource_id="1sN1r8YdIMCxH8ux-RdeiYA5p2O2TNDkZ"
municipalities_resource_id="1XkAlUjXR2RYLDaNEOfZWS63VmfFYi91x"
airports_resource_id="1k1NcL5XOFpMz0jX-KKl0_PjF-4ReOtYH"
cities_resource_id="1KPNtXMNCAIH2wgGeWQYnCbBfakhlOJz5"

flightpoints_converter_resource_id="16GRpXquKNhOEucczoXM58xSVhJkWbGRb"
flighttrips_converter_resource_id="1ydssKxKXJ6X1ulmyeebwxejmtHAzVd3-"
regions_converter_resource_id="1XxefNd4gATjNWW6bPtCDi47KfeLVejNf"
airports_converter_resource_id="1EpBUYnO3ztcX1jlYddZ3-WEv3iFvkC2i"
cities_converter_resource_id="1dngxE8cIS38H5PrWeDG2jHri76PENmL0"

user="felix"
user_password="master"
database="aviation_data"

# Get number of geomesa nodes
instances="$(gcloud compute instances list)"
value="geomesa"
nodeNumber="$(echo -n $instances | grep -Fo $value | wc -l)"
instanceGroupName=${name:-geomesa-node}

declare -a nodes
declare -a nodesExternal
workers=""

# get an array of machine internal ips
for (( i=1; i <= nodeNumber; ++i ))
do
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodes[i]=$nodeInternalIp
nodesExternal[i]=$nodeExternalIp
done

# Zookeeper Installation
for (( i=1; i <= nodeNumber; ++i ))
do 
firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"

echo "Installing Zookeeper on $currentInstanceName."

if [[ $i -eq 1 ]];then

firstZone=$zone
seedIp=$nodeInternalIp
seedIpExternal=$nodeExternalIp
firstInstanceName=$currentInstanceName

fi

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get update && sudo apt-get install -y openjdk-11-jdk" > /dev/null

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo wget https://dlcdn.apache.org/zookeeper/zookeeper-3.8.4/apache-zookeeper-3.8.4-bin.tar.gz && tar xzf apache-zookeeper-3.8.4-bin.tar.gz" > /dev/null
gcloud compute ssh $currentInstanceName --zone $zone -- "cd $ZOOKEEPER_HOME && cp conf/zoo_sample.cfg $ZOOKEEPER_CONF_FILE && echo \"$ZOOKEEPER_CONF\" > $ZOOKEEPER_CONF_FILE"

index=1
for node in "${nodes[@]}"; do 
    gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"server.$index=$node:2888:3888\" >> $ZOOKEEPER_CONF_FILE"
    index=$((index+1))
done

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo mkdir -p /var/lib/zookeeper && sudo chmod -R 777 /var/lib/zookeeper && echo $i > /var/lib/zookeeper/myid"
gcloud compute ssh $currentInstanceName --zone $zone -- "bash $ZOOKEEPER_HOME/bin/zkServer.sh start"

done

# HDFS Installation
for (( i=1; i <= 1; ++i ))
do 
firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"

echo "Installing HDFS on $currentInstanceName."

# Adding machines to host files of all instances
for (( j=1; j <= nodeNumber; ++j )); 
do 
currentInstanceName="${instanceGroupName}-$j"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
index=0
for node in "${nodes[@]}"; do 
    if [[ $index -eq 0 ]];then
        gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"$node node-master\" | sudo tee -a /etc/hosts > /dev/null"
    fi
    if [[ $index -gt 0 ]];then
        gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"$node node${index}\" | sudo tee -a /etc/hosts > /dev/null"
    fi
    index=$((index+1))
done
done
index=0
for node in "${nodes[@]}"; do 
    if [[ $index -gt 0 ]];then
        workers+="node${index}\n"
    fi
    index=$((index+1))
done


currentInstanceName="${instanceGroupName}-1"

default_key="Host *
    IdentityFile ~/.ssh/master"

gcloud compute ssh $currentInstanceName --zone $zone -- "ssh-keygen -b 4096 -f $HOME/.ssh/master -N \"\" && cat $HOME/.ssh/master.pub >> $HOME/.ssh/authorized_keys && echo \"$default_key\" > $HOME/.ssh/config"
pub_key=$(gcloud compute ssh $currentInstanceName --zone $zone -- "cat $HOME/.ssh/master.pub") 

# Copy public ssh keys to worker machines and add to authorized keys
for (( j=2; j <= nodeNumber; ++j ))
do 
currentInstanceName="${instanceGroupName}-$j"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
echo "Adding public key to node$((j-1))"
gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"${pub_key}\" > $HOME/.ssh/master.pub && cat $HOME/.ssh/master.pub >> $HOME/.ssh/authorized_keys"    
done

currentInstanceName="${instanceGroupName}-1"

echo "Downloading HDFS"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo wget https://dlcdn.apache.org/hadoop/common/hadoop-${HDFS_VERSION}/hadoop-${HDFS_VERSION}.tar.gz && tar xzf hadoop-${HDFS_VERSION}.tar.gz" > /dev/null
gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"export HADOOP_HOME=$HDFS_HOME\" >> $HOME/.bashrc && echo 'export PATH=\$PATH:${HDFS_HOME}/bin:${HDFS_HOME}/sbin' >> $HOME/.bashrc && echo 'PATH=$HDFS_HOME/bin:$HDFS_HOME/sbin:\$PATH' >> $HOME/.profile && echo \"export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64\" >> $HOME/.bashrc"
gcloud compute ssh $currentInstanceName --zone $zone -- "sed -i 's|# export JAVA_HOME=|export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64|' $HDFS_HOME/etc/hadoop/hadoop-env.sh"

HDFS_CORE="<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://node-master:9000</value>
    </property>
</configuration>"

HDFS_SITE="<configuration>
<property>
        <name>dfs.namenode.name.dir</name>
        <value>$HOME/data/nameNode</value>
</property>
<property>
        <name>dfs.datanode.data.dir</name>
        <value>$HOME/hadoop/data/dataNode</value>
</property>
<property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>"

HDFS_MAPRED="
<configuration>
<property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
</property>
<property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=$HDFS_HOME</value>
</property>
<property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=$HDFS_HOME</value>
</property>
<property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=$HDFS_HOME</value>
</property>
<property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>1024</value>
</property>
<property>
        <name>mapreduce.map.memory.mb</name>
        <value>512</value>
</property>
<property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>512</value>
</property>
</configuration>"

YARN_SITE="<configuration>
<property>
        <name>yarn.acl.enable</name>
        <value>0</value>
</property>
<property>
        <name>yarn.resourcemanager.hostname</name>
        <value>$seedIp</value>
</property>
<property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
</property>
<property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>3072</value>
</property>
<property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>3072</value>
</property>
<property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>256</value>
</property>
<property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
</property>
</configuration>"

gcloud compute ssh $currentInstanceName --zone $zone -- "
  echo \"$HDFS_CORE\" > $HDFS_CONF_DIR/core-site.xml && echo \"$HDFS_SITE\" > $HDFS_CONF_DIR/hdfs-site.xml &&
  echo \"$HDFS_MAPRED\" > $HDFS_CONF_DIR/mapred-site.xml && echo \"$YARN_SITE\" > $HDFS_CONF_DIR/yarn-site.xml"

gcloud compute ssh $currentInstanceName --zone $zone -- "echo -e \"$workers\" > $HDFS_CONF_DIR/workers"    
    
echo "Attempting to copy configurations from master to worker nodes."
gcloud compute ssh $firstInstanceName --zone $firstZone -- "cd $HOME && scp -o StrictHostKeyChecking=no -i $HOME/.ssh/master hadoop-*.tar.gz node1:$HOME && scp -o StrictHostKeyChecking=no -i $HOME/.ssh/master hadoop-*.tar.gz node2:$HOME"

for (( j=2; j<= $nodeNumber; ++j )); do
    currentInstanceName="${instanceGroupName}-$j"
    zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
    gcloud compute ssh $currentInstanceName --zone $zone -- "tar -xzf hadoop-${HDFS_VERSION}.tar.gz"
done

for node in node1 node2; do
    echo "Attempting to copy configurations from master to $node."
    gcloud compute ssh $firstInstanceName --zone $firstZone -- "scp -o StrictHostKeyChecking=no -i $HOME/.ssh/master $HDFS_HOME/etc/hadoop/* $node:$HDFS_HOME/etc/hadoop/"
done
sleep 5
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.profile; cd $HDFS_HOME/bin && hdfs namenode -format && cd $HDFS_HOME/sbin && start-dfs.sh"

done


# Accumulo Installation
for (( i=1; i <= nodeNumber; ++i )); do 
firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"

echo "Installing Accumulo on $currentInstanceName."
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo wget https://dlcdn.apache.org/accumulo/${ACCUMULO_VERSION}/accumulo-${ACCUMULO_VERSION}-bin.tar.gz && tar xzf accumulo-${ACCUMULO_VERSION}-bin.tar.gz" > /dev/null
gcloud compute ssh $currentInstanceName --zone $zone -- "echo 'export PATH=\$PATH:${ACCUMULO_HOME}/bin' >> \$HOME/.bashrc && source \$HOME/.bashrc"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt update && sudo apt install -y build-essential"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sysctl vm.swappiness=10 && source ~/.bashrc; $ACCUMULO_HOME/bin/accumulo-util build-native"


zookeeper_hosts=()
for node in "${nodes[@]}"; do 
    zookeeper_hosts+=( "$node:2181" )
done
zookeeper_hosts_conf=$(IFS=','; echo "${zookeeper_hosts[*]}")

namenode=${nodes[1]}

# Configure Accumulo main Properties
gcloud compute ssh $currentInstanceName --zone $zone -- "
  sed -i \"s|instance.volumes=hdfs://localhost:8020/accumulo|instance.volumes=hdfs://$namenode:9000/accumulo|g\" $ACCUMULO_CONF_DIR/accumulo.properties; 
  sed -i \"s|instance.zookeeper.host=localhost:2181|instance.zookeeper.host=$namenode:2181|g\" $ACCUMULO_CONF_DIR/accumulo.properties"

# Configure Accumulo environment
gcloud compute ssh $currentInstanceName --zone $zone -- "
  sed -i 's|^\(ACCUMULO_LOG_DIR=\).*|\1${ACCUMULO_HOME}/logs|g' ${ACCUMULO_CONF_DIR}/accumulo-env.sh;
  sed -i 's|^\(HADOOP_HOME=\).*|\1${HDFS_HOME}|g' ${ACCUMULO_CONF_DIR}/accumulo-env.sh;
  sed -i 's|^\(HADOOP_CONF_DIR=\).*|\1${HDFS_HOME}/etc/hadoop|g' ${ACCUMULO_CONF_DIR}/accumulo-env.sh;
  sed -i 's|^\(ZOOKEEPER_HOME=\).*|\1${ZOOKEEPER_HOME}|g' ${ACCUMULO_CONF_DIR}/accumulo-env.sh
  sed -i 's/-Xmx768m/-Xmx3072m/' ${ACCUMULO_CONF_DIR}/accumulo-env.sh
  sed -i 's/-Xms768m/-Xms3072m/' ${ACCUMULO_CONF_DIR}/accumulo-env.sh
  sed -i 's/-Xmx512m/-Xmx2048m/' ${ACCUMULO_CONF_DIR}/accumulo-env.sh
  sed -i 's/-Xms512m/-Xms2048m/' ${ACCUMULO_CONF_DIR}/accumulo-env.sh"

# Configure the Accumulo client properties
gcloud compute ssh $currentInstanceName --zone $zone -- "
  sed -i 's|^\(instance.name=\).*|\1accumulo-node-${i}|g' ${ACCUMULO_CONF_DIR}/accumulo-client.properties;
  sed -i 's|^\(instance.zookeepers=\).*|\1$namenode:2181|g' ${ACCUMULO_CONF_DIR}/accumulo-client.properties;
  sed -i 's|^\(auth.principal=\).*|\1felix|g' ${ACCUMULO_CONF_DIR}/accumulo-client.properties;
  sed -i 's|^\(auth.token=\).*|\1master|g' ${ACCUMULO_CONF_DIR}/accumulo-client.properties"

cluster_conf="manager:
  - ${nodes[1]}

monitor:
  - ${nodes[1]}

gc:
  - ${nodes[1]}

tserver:
  - node1
  - node2
  
#sserver:
#  - default:
#    - localhost
#
#compaction:
#  coordinator:
#    - localhost
#  compactor:
#    - q1:
# The following are used by the accumulo-cluster script to determine how many servers
# to start on each host. If the following variables are not set, then they default to 1.
# If the environment variable NUM_TSERVERS is set when running accumulo_cluster
# then its value will override what is set in this file for tservers_per_host. Likewise if
# NUM_SSERVERS or NUM_COMPACTORS are set then it will override sservers_per_host and
# compactors_per_host.
#
tservers_per_host: 1
#sservers_per_host: 
# - default: 1
#compactors_per_host:
# - q1: 1
# - q2: 1 
"
# accumulo manager needs to create configuration for the cluster
if [[ $i -eq 1 ]];then
    gcloud compute ssh $currentInstanceName --zone $zone -- "source ~/.bashrc; $ACCUMULO_HOME/bin/accumulo-cluster create-config && echo \"${cluster_conf}\" > $ACCUMULO_CONF_DIR/cluster.yaml"

fi

done 

for (( i=1; i <= nodeNumber; ++i )); do

firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"

echo "Installing Geomesa on $currentInstanceName."

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo wget \"https://github.com/locationtech/geomesa/releases/download/geomesa-${GEOMESA_TAG}/geomesa-accumulo_${GEOMESA_VERSION}-bin.tar.gz\" && tar xvf geomesa-accumulo_${GEOMESA_VERSION}-bin.tar.gz" > /dev/null

if [[ $i -eq 1 ]];then
gcloud compute ssh $currentInstanceName --zone $zone -- "echo 'export PATH=\$PATH:${GEOMESA_HOME}/bin' >> ~/.bashrc && source ~/.bashrc && yes | $GEOMESA_HOME/bin/install-shapefile-support.sh && yes | $GEOMESA_HOME/bin/install-dependencies.sh"
fi

# gcloud compute ssh $currentInstanceName --zone $zone -- "source ~/.bashrc && $HDFS_HOME/bin/hadoop fs -mkdir -p /accumulo/classpath/myNamespace"
# gcloud compute ssh $currentInstanceName --zone $zone -- "source ~/.bashrc && $HDFS_HOME/bin/hadoop fs -put ~/geomesa-accumulo_${GEOMESA_VERSION}/dist/accumulo/geomesa-accumulo-distributed-runtime_${GEOMESA_VERSION}.jar /accumulo/classpath/myNamespace/"
# gcloud compute ssh $currentInstanceName --zone $zone -- "source ~/.bashrc && $ACCUMULO_HOME/bin/accumulo shell -u root"
# shell_commands="createnamespace myNamespace
# grant NameSpace.CREATE_TABLE -ns myNamespace -u root
# config -s general.vfs.context.classpath.myNamespace=hdfs://node-master:9000/accumulo/classpath/myNamespace/[^.].*.jar
# config -ns myNamespace -s table.classpath.context=myNamespace"

gcloud compute ssh $currentInstanceName --zone $zone -- "mv $GEOMESA_HOME/dist/accumulo/geomesa-accumulo-distributed-runtime_${GEOMESA_VERSION}.jar $ACCUMULO_HOME/lib"

done

gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc; $ACCUMULO_HOME/bin/accumulo init --instance-name accumulo-node-1 --password $user_password && $ACCUMULO_HOME/bin/accumulo-cluster start"
sleep 5
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && yes | $GEOMESA_HOME/bin/geomesa-accumulo"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "sudo apt install -y python3-pip && pip install gdown"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "echo 'downloading flightdata and converter' && $HOME/.local/bin/gdown $flightpoints_resource_id && $HOME/.local/bin/gdown $flighttrips_resource_id && $HOME/.local/bin/gdown $flightpoints_converter_resource_id && $HOME/.local/bin/gdown $flighttrips_converter_resource_id && $HOME/.local/bin/gdown $regions_converter_resource_id && $HOME/.local/bin/gdown $counties_resource_id && $HOME/.local/bin/gdown $municipalities_resource_id && $HOME/.local/bin/gdown $districts_resource_id && $HOME/.local/bin/gdown $cities_resource_id && $HOME/.local/bin/gdown $airports_resource_id && $HOME/.local/bin/gdown $airports_converter_resource_id && $HOME/.local/bin/gdown $cities_converter_resource_id"
# split datasets so more than 1 thread can insert the data into accumulo simultaneously
gcloud compute ssh $firstInstanceName --zone $firstZone -- "split -l 5000000 FlightPointsGeomesa.csv splitted_flight_points_ && split -l 35000 FlightTripsGeomesa.csv"

# geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root  -p master -c pointcatalog -f flightpoints -s "timestamp:Date,icao:String:index=full,latitude:Double,longitude:Double,geom:Point:srid=4326,velocity:Double,heading:Double,vertrate:Double,callsign:String,onground:Boolean,alert:Boolean,spi:Boolean,squawk:Integer,altitude:Double"
# geomesa-accumulo ingest -C ~/flightPoints.converter -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature -t 2 ~/splitted_flight_data_*
# geomesa-accumulo delete-features -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature
# geomesa-accumulo remove-schema -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature
# geomesa-accumulo stats-count -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature
# geomesa-accumulo export -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f flightpointfeature -m 50

gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f flightpoints -s \"flightId:Integer,timestamp:Date,airplaneType:String,origin:String,destination:String,track:String,latitude:Double,longitude:Double,altitude:Double,geom:Point:srid=4326\""
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f counties -s \"name:String,geom:Polygon:srid=4326\""
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f districts -s \"name:String,geom:Polygon:srid=4326\""
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f municipalities -s \"name:String,geom:Polygon:srid=4326\""
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f airports -s \"iata:String,icao:String,name:String,country:String,city:String\""
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f cities -s \"area:Double,lat:Double,long:Double,district:String,name:String,population:Integer,geom:Point:srid=4326\""

gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo ingest -C ~/flightPoints.converter -c flightcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpoints -t 1 ~/FlightPointsGeomesa.csv"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo ingest -C ~/regions.converter -c flightcatalog -i accumulo-node-1 -z localhost -u root -p master -f counties -t 1 ~/geomesa-counties.csv"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo ingest -C ~/regions.converter -c flightcatalog -i accumulo-node-1 -z localhost -u root -p master -f municipalities -t 1 ~/geomesa-municipalities.csv"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo ingest -C ~/regions.converter -c flightcatalog -i accumulo-node-1 -z localhost -u root -p master -f districts -t 1 ~/geomesa-districts.csv"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo ingest -C ~/airports.converter -c flightcatalog -i accumulo-node-1 -z localhost -u root -p master -f airports -t 1 ~/airports.csv"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "source ~/.bashrc && $GEOMESA_HOME/bin/geomesa-accumulo ingest -C ~/cities.converter -c flightcatalog -i accumulo-node-1 -z localhost -u root -p master -f cities -t 1 ~/cities.csv"




# geomesa-accumulo ingest -C ~/dfsFlightPoints.converter -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature -t 2 ~/splitted_flight_data_*

# geomesa-accumulo delete-features -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature
# geomesa-accumulo remove-schema -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature
# geomesa-accumulo stats-count -c flightpointcatalog -i accumulo-node-1 -z localhost -u root -p master -f flightpointfeature
# geomesa-accumulo export -i accumulo-node-1 -z localhost -u root -p master -c flightpointcatalog -f flightpointfeature -m 50

# geomesa-accumulo create-schema -i accumulo-node-1 -z localhost -u root -p master -c flightcatalog -f flighttrips -s "flightId:Integer,airplaneType:String,origin:String,destination:String,timestamp:List[Date],trip:MultiPoint:srid=4326,altitude:List[Double]"
# geomesa-accumulo ingest -C ~/flightTrips.converter -c flightcatalog -i accumulo-node-1 -z localhost -u root -p master -f flighttrips -t 1 ~/FlightTripsGeomesa.csv
