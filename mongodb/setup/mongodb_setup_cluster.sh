#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

version=8
config_file="/etc/mongod.conf"
log_dir="/var/log/mongodb"
data_dir="/var/lib/mongodb"
conf_data_dir="/var/lib/mongoConfDB"
repl_set_name="confReplSet"

flight_data_resource_id="1REu74vRj6tsoPKO7J7bfOEjjdaWEY4Dm"
cities_resource_id="1KPNtXMNCAIH2wgGeWQYnCbBfakhlOJz5"
municipalities_resource_id="1IxS8b4RaNe9glfdk4ZurXrZiZFnJNhC5"
counties_resource_id="1KkNU4iwMeIHDoBMhFI4eJkruTFlm3xAP"
districts_resource_id="1psjLKgaciSXmVBs-DITOPHNE5UMfHmss"
airports_resource_id="1ipwMDY8g9a0yrLG78IJAL3B-wHnoobho"

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
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get install -y mongodb-org && sudo chmod 777 $config_file && echo -e \"\nsecurity:\n  authorization: enabled\" >> /etc/mongod.conf"

# gcloud compute ssh $currentInstanceName --zone $zone -- "sudo bash -c \"openssl rand -base64 756 > /etc/mongodb-keyfile\" && sudo chmod 600 /etc/mongodb-keyfile && sudo chown mongodb:mongodb /etc/mongodb-keyfile"
# gcloud compute ssh $currentInstanceName --zone $zone -- "sudo scp /etc/mongodb-keyfile felix@:${ipAdresses[2]}/etc/mongodb-keyfile && sudo scp /etc/mongodb-keyfile felix@:${ipAdresses[3]}/etc/mongodb-keyfile"

gcloud compute scp ~/Documents/mongodb-keyfile "$currentInstanceName":~/mongodb-keyfile "--zone=$zone"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo mv ~/mongodb-keyfile /etc/mongodb-keyfile && sudo chmod 600 /etc/mongodb-keyfile"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo mkdir -p $conf_data_dir && sudo chmod 777 $conf_data_dir"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo chmod 777 $log_dir && sudo chmod 777 $data_dir && sudo mongod --configsvr --replSet $repl_set_name --dbpath $conf_data_dir --bind_ip 0.0.0.0 --auth --keyFile /etc/mongodb-keyfile --fork --logpath /var/log/mongodb/mongod.log"

done

# config_change="db.adminCommand( {
#    transitionFromDedicatedConfigServer: 1
# } )"

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

# gcloud compute ssh "$currentInstanceName" --zone "$zone" -- "sudo mongos --configdb $repl_set_name/${ipAdresses[1]}:27019,${ipAdresses[2]}:27019,${ipAdresses[3]}:27019 --bind_ip 0.0.0.0 --keyFile /etc/mongodb-keyfile --fork --logpath /var/log/mongodb/mongos.log"

# Start sharding servers on every instance
gcloud compute ssh "$currentInstanceName" --zone $zone -- "
sudo mongod --shardsvr --replSet shard${i}ReplSet --dbpath $data_dir --port 27018 --bind_ip 0.0.0.0 --auth --keyFile /etc/mongodb-keyfile --fork --logpath /var/log/mongodb/mongod.log --setParameter indexMaxNumGeneratedKeysPerDocument=1000000
"

# Start mongos (routers) on every instance
gcloud compute ssh "$currentInstanceName" --zone "$zone" -- "sudo mongos --configdb $repl_set_name/${ipAdresses[1]}:27019,${ipAdresses[2]}:27019,${ipAdresses[3]}:27019 --bind_ip 0.0.0.0 --port 27017 --keyFile /etc/mongodb-keyfile --fork --logpath /var/log/mongodb/mongos.log"

if [ "$i" -eq 1 ]; then
gcloud compute ssh "$currentInstanceName" --zone "$zone" -- \
"mongosh --eval 'db.getSiblingDB(\"admin\").createUser({ user: \"$user\", pwd: \"$user_password\", roles: [ { role: \"root\", db: \"admin\" } ] });'"

fi

gcloud compute ssh "$currentInstanceName" --zone "$zone" -- <<EOF
mongosh --host localhost --port 27018 --eval '
rs.initiate({
    _id: "shard${i}ReplSet",
    members: [
        { _id: 0, host: "${ipAdresses[$i]}:27018" }
    ]
})'
EOF

done

gcloud compute ssh "$firstInstanceName" --zone "$zone" -- <<EOF
mongosh --host localhost --port 27017 -u $user -p $user_password --eval '
sh.addShard("shard1ReplSet/${ipAdresses[1]}:27018");
sh.addShard("shard2ReplSet/${ipAdresses[2]}:27018");
sh.addShard("shard3ReplSet/${ipAdresses[3]}:27018");
'
EOF

gcloud compute ssh "$firstInstanceName" --zone "$firstZone" -- "sudo apt install -y python3-pip && pip install gdown"

gcloud compute ssh "$firstInstanceName" --zone "$firstZone" -- "echo 'downloading flightdata' && ~/.local/bin/gdown $flight_data_resource_id && sudo mv /home/felix/FlightPointsMobilityDB.csv /tmp/FlightPoints.csv"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "echo 'downloading regional Data' && sudo mkdir /tmp/regData && cd /tmp/regData && sudo chmod 777 . && ~/.local/bin/gdown $cities_resource_id && ~/.local/bin/gdown $municipalities_resource_id && ~/.local/bin/gdown $counties_resource_id && ~/.local/bin/gdown $districts_resource_id &&  ~/.local/bin/gdown $airports_resource_id"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"flightpoints\")'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"flightpoints_ts\")'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"cities\")'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"airports\")'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"districts\")'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"counties\")'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"municipalities\")'"


gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'sh.enableSharding(\"$database\")'"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.flightpoints.createIndex({ flightId: \"hashed\" });' && mongosh -u $user -p $user_password --eval 'sh.shardCollection(\"${database}.flightpoints\", { flightId: \"hashed\" });'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.flightpoints.createIndex({ name: \"hashed\" });' && mongosh -u $user -p $user_password --eval 'sh.shardCollection(\"${database}.cities\", { name: \"hashed\" });'"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.flightpoints.createIndex({ IATA: \"hashed\" });' && mongosh -u $user -p $user_password --eval 'sh.shardCollection(\"${database}.airports\", { IATA: \"hashed\" });'"



gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongoimport --db=$database --collection=flightpoints --type=csv --headerline --file=/tmp/FlightPoints.csv --batchSize=5000 --numInsertionWorkers=2 --username $user --password $user_password --authenticationDatabase admin"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongoimport --db=$database --collection=cities --type=csv --headerline --file=/tmp/regData/cities.csv --batchSize=5000 --numInsertionWorkers=2 --username $user --password $user_password --authenticationDatabase admin"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongoimport --db=$database --collection=airports --type=csv --headerline --file=/tmp/regData/airports_mongo.csv --batchSize=5000 --numInsertionWorkers=2 --username $user --password $user_password --authenticationDatabase admin"
