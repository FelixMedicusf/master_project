#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

version=8
config_file="/etc/mongod.conf"
log_dir="/var/log/mongodb"
data_dir="/var/lib/mongodb"
repl_set_name="configServerReplicaSet"

flight_data_resource_id="1REu74vRj6tsoPKO7J7bfOEjjdaWEY4Dm"
cities_resource_id="1KPNtXMNCAIH2wgGeWQYnCbBfakhlOJz5"
municipalities_resource_id="1IxS8b4RaNe9glfdk4ZurXrZiZFnJNhC5"
counties_resource_id="1KkNU4iwMeIHDoBMhFI4eJkruTFlm3xAP"
districts_resource_id="1psjLKgaciSXmVBs-DITOPHNE5UMfHmss"
airports_resource_id="1k1NcL5XOFpMz0jX-KKl0_PjF-4ReOtYH"

db_host=""
db_port=""

user="felix"
user_password="master"
database="aviation_data"

# Get number of MongoDB nodes
instances="$(gcloud compute instances list)"
value="mongodb"
nodeNumber="$(echo -n $instances | grep -Fo $value | wc -l)"

instanceGroupName=${name:-mongodb-node}

declare -a ipAdresses
declare -a ipAdressesExternal


# Loop through all deployed nodes to provision them MongoDB instances
for (( i=1; i <= nodeNumber; ++i ))
do 
firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
firstZone="$(gcloud compute instances list --filter="name=$firstInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"
ipAdresses[i]=$nodeInternalIp

echo "Provisioning $currentInstanceName"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get install gnupg curl && curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor"
gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse\" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list && sudo apt-get update"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get install -y mongodb-org"

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo chmod 777 $log_dir && sudo chmod 777 $data_dir && sudo chmod 777 $config_file && sudo mongod --configsvr --replSet $repl_set_name --dbpath /var/lib/mongodb --bind_ip 0.0.0.0 --fork --logpath /var/log/mongodb/mongod.log"

done



config_change="db.adminCommand( {
   transitionFromDedicatedConfigServer: 1
} )"


gcloud compute ssh "$firstInstanceName" --zone "$zone" -- <<EOF
mongosh --host localhost --port 27019 --eval '
rs.initiate({
  _id: "$repl_set_name",
  configsvr: true,
  members: [
    { _id: 0, host: "${ipAdresses[1]}:27019" },
    { _id: 1, host: "${ipAdresses[2]}:27019" },
    { _id: 2, host: "${ipAdresses[3]}:27019" }
  ]
})'
EOF


for (( i=1; i <= nodeNumber; ++i ))
do 
firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"
ipAdresses[i]=$nodeInternalIp

echo "Provisioning $currentInstanceName"

gcloud compute ssh "$currentInstanceName" --zone "$zone" -- "sudo mongos --configdb $repl_set_name/${ipAdresses[1]}:27019,${ipAdresses[2]}:27019,${ipAdresses[3]}:27019 --bind_ip 0.0.0.0 --fork --logpath /var/log/mongodb/mongos.log"
gcloud compute ssh "$currentInstanceName" --zone "$zone" -- "echo \"$config_change\" | mongosh --host localhost --port 27017 --eval"

done

gcloud compute ssh $firstInstanceName --zone $firstZone -- "sudo apt install -y python3-pip && pip install gdown"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "echo 'downloading flightdata' && ~/.local/bin/gdown $flight_data_resource_id && sudo mv /home/felix/FlightPointsMobilityDB.csv /tmp/FlightPoints.csv"


gcloud compute ssh $firstInstanceName --zone $firstZone -- "echo 'downloading regional Data' && sudo mkdir /tmp/regData && cd /tmp/regData && sudo chmod 777 . && ~/.local/bin/gdown $cities_resource_id && ~/.local/bin/gdown $municipalities_resource_id && ~/.local/bin/gdown $counties_resource_id && ~/.local/bin/gdown $districts_resource_id &&  ~/.local/bin/gdown $airports_resource_id"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongoimport --db=aviation_data --collection=flightpoints --type=csv --headerline --file=/tmp/FlightPoints.csv --batchSize=5000 --numInsertionWorkers=2"

#for (( i=1; i <= nodeNumber; ++i ))
#do 
#firstInstanceName="${instanceGroupName}-1"
#currentInstanceName="${instanceGroupName}-$i"
#zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
#nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
#nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"


#done

#gcloud compute ssh $currentInstanceName --zone $zone -- "sudo systemctl start mongod"
