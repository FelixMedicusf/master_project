#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

cassandra_version=4.0.12
main_config_file="/etc/cassandra/cassandra.yaml"
env_config_file="/etc/cassandra/cassandra-env.sh"
topology_config_file="/etc/cassandra/cassandra-topology.properties"
log_file="/var/log/cassandra/system.log"

# pg_hba_file="/etc/postgresql/$pg_version/main/pg_hba.conf"
# pg_pass_file="$HOME/.pgpass"

# data_path="Flight_Points_Actual_20220601_20220630.csv"
google_drive_resource_id="1Gnu3-dA4877iGZeU6-Yjhl5v5mS1lu-3"

user="felix"
user_password="master"
database="aviation_data"

db_host="localhost"
db_port="5432"

# Get number of cassandra nodes
instances="$(gcloud compute instances list)"
value="cassandra"
nodeNumber="$(echo -n $instances | grep -Fo $value | wc -l)"

instanceGroupName=${name:-cassandra-node}

declare -a ipAdresses

# Loop through all deployed nodes to provision them Cassandra instances
for (( i=1; i <= $nodeNumber; ++i ))
do 
firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"

echo "Installing Java and Cassandra on $currentInstanceName."

if [[ $i -eq 1 ]];then

firstZone=$zone
seedIp=$nodeInternalIp
seedIpExternal=$nodeExternalIp
firstInstanceName=$currentInstanceName

fi

# Install cassandra
gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"deb [signed-by=/etc/apt/keyrings/apache-cassandra.asc] https://debian.cassandra.apache.org 40x main\" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo curl -o /etc/apt/keyrings/apache-cassandra.asc https://downloads.apache.org/cassandra/KEYS"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt update && sudo apt install -y openjdk-11-jdk"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get -y install cassandra"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo service cassandra stop && sudo rm -rf /var/lib/cassandra/data/system/*" 

# set these configs: cluster_name, seeds, storage_port, listen_address, native transport port
# gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^  CASSANDRA_NUM_TOKENS: .*/  CASSANDRA_NUM_TOKENS: 16/\" $main_config_file"
# gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^  CASSANDRA_STORAGE_PORT: .*/  CASSANDRA_STORAGE_PORT: 7000/\" $main_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^cluster_name: .*/cluster_name: 'GeoMesa Cluster'/\" $main_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^# broadcast_rpc_address: .*/broadcast_rpc_address: $nodeExternalIp/\" $main_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^# broadcast_address: .*/broadcast_address: $nodeInternalIp/\" $main_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^listen_address: .*/listen_address: $nodeInternalIp/\" $main_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^rpc_address: .*/rpc_address: 0.0.0.0/\" $main_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/^endpoint_snitch: .*/endpoint_snitch: 'GoogleCloudSnitch'/\" $main_config_file"

ipAdresses[$i]="$nodeInternalIp:7000"

done

IFS=","

seeds="${ipAdresses[*]}"

for (( i=1; i <= $nodeNumber; ++i ))
do 
    currentInstanceName="${instanceGroupName}-$i"
    gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i.bak 's/seeds: \".*\"/seeds: \"$seeds\"/' $main_config_file"
done 

for (( i=1; i <= $nodeNumber; ++i ))
do 
    currentInstanceName="${instanceGroupName}-$i"
    gcloud compute ssh $currentInstanceName --zone $zone -- "sudo systemctl start cassandra"

done 


echo "Installing GeoMesa on $currentInstanceName."

for (( i=1; i <= 1; ++i))
do 
    currentInstanceName="${instanceGroupName}-$i"
    gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"export TAG='5.0.1'\" >> ~/.profile && echo \"export VERSION='2.12-5.0.1'\" >> ~/.profile"
    gcloud compute ssh $currentInstanceName --zone $zone -- "echo 'export CASSANDRA_HOME=/usr/share/cassandra/' >> ~/.profile && export CASSANDRA_HOME=/usr/share/cassandra/"
    gcloud compute ssh $currentInstanceName --zone $zone -- "sudo wget \"https://github.com/locationtech/geomesa/releases/download/geomesa-5.0.1/geomesa-cassandra_2.12-5.0.1-bin.tar.gz\""
    gcloud compute ssh $currentInstanceName --zone $zone -- "tar xvf geomesa-cassandra_2.12-5.0.1-bin.tar.gz && cd geomesa-cassandra_2.12-5.0.1"
    gcloud compute ssh $currentInstanceName --zone $zone -- "cd geomesa-cassandra_2.12-5.0.1 && echo \"yes\" | ./bin/install-shapefile-support.sh"
    gcloud compute ssh $currentInstanceName --zone $zone -- "cd geomesa-cassandra_2.12-5.0.1 && echo \"yes\" | ./bin/geomesa-cassandra"
    echo "Create Keyspace 'mykeyspace' on Cassandra cluster after waiting time."
    #sleep 30
    #gcloud compute ssh $currentInstanceName --zone $zone -- "cqlsh -e \"CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor' : 3};\""

done

