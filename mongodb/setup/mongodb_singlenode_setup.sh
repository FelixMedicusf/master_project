#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

version=8
config_file="/etc/mongod.conf"
log_dir="/var/log/mongodb"
data_dir="/var/lib/mongodb"
conf_data_dir="/var/lib/mongoConfDB"

flight_data_resource_id="1REu74vRj6tsoPKO7J7bfOEjjdaWEY4Dm"
flight_data_resource_id_large="1Q7Yio3eUulzE6jl8J1zkWJiLqrHOzSUv"
cities_resource_id="1KPNtXMNCAIH2wgGeWQYnCbBfakhlOJz5"
municipalities_resource_id="1IxS8b4RaNe9glfdk4ZurXrZiZFnJNhC5"
counties_resource_id="1KkNU4iwMeIHDoBMhFI4eJkruTFlm3xAP"
districts_resource_id="1psjLKgaciSXmVBs-DITOPHNE5UMfHmss"
airports_resource_id="1ipwMDY8g9a0yrLG78IJAL3B-wHnoobho"

user="felix"
user_password="master"
database="aviation_data"

instanceName="mongodb-node-1"
zone="$(gcloud compute instances list --filter="name=$instanceName" --format "get(zone)" | awk -F/ '{print $NF}')"

echo "Provisioning $instanceName"

gcloud compute ssh $instanceName --zone "$zone" -- "sudo apt-get update && sudo apt-get install -y gnupg curl"
gcloud compute ssh $instanceName --zone $zone -- "curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor"
gcloud compute ssh $instanceName --zone $zone -- "echo \"deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse\" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list && sudo apt-get update"

gcloud compute ssh $instanceName --zone $zone -- "sudo apt-get install -y mongodb-org && sudo chmod 777 $config_file && echo -e \"\nsecurity:\n  authorization: enabled\" >> /etc/mongod.conf"
gcloud compute ssh $instanceName --zone "$zone" -- "sudo mongod --dbpath $data_dir --bind_ip 0.0.0.0 --auth --fork --logpath /var/log/mongodb/mongod.log --setParameter indexMaxNumGeneratedKeysPerDocument=1000000 --setParameter internalQueryMaxAllowedDensifyDocs=100000000"

sleep 10

gcloud compute ssh "$instanceName" --zone "$zone" -- \
"mongosh --eval 'db.getSiblingDB(\"admin\").createUser({ user: \"$user\", pwd: \"$user_password\", roles: [ { role: \"root\", db: \"admin\" } ] });'"

echo "MongoDB setup completed on $instanceName with user '$user' and password '$user_password'."

gcloud compute ssh "$instanceName" --zone "$zone" -- "sudo apt install -y python3-pip && pip install gdown"

gcloud compute ssh "$instanceName" --zone "$zone" -- "echo 'downloading flightdata' && ~/.local/bin/gdown $flight_data_resource_id_large && sudo mv /home/felix/FlightPointsMobilityDBlarge.csv /tmp/FlightPointslarge.csv"
gcloud compute ssh $instanceName --zone "$zone" -- "echo 'downloading regional Data' && sudo mkdir /tmp/regData && cd /tmp/regData && sudo chmod 777 . && ~/.local/bin/gdown $cities_resource_id && ~/.local/bin/gdown $municipalities_resource_id && ~/.local/bin/gdown $counties_resource_id && ~/.local/bin/gdown $districts_resource_id &&  ~/.local/bin/gdown $airports_resource_id"

gcloud compute ssh $instanceName --zone "$zone" -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"flightpoints\")'"
gcloud compute ssh $instanceName --zone "$zone" -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"cities\")'"
gcloud compute ssh $instanceName --zone "$zone" -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"airports\")'"
gcloud compute ssh $instanceName --zone "$zone" -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"districts\")'"
gcloud compute ssh $instanceName --zone "$zone" -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"counties\")'"
gcloud compute ssh $instanceName --zone "$zone" -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"municipalities\")'"

gcloud compute ssh $instanceName --zone "$zone" -- "mongoimport --db=$database --collection=flightpoints --type=csv --headerline --file=/tmp/FlightPointslarge.csv --batchSize=100000 --numInsertionWorkers=8 --username $user --password $user_password --authenticationDatabase admin"
gcloud compute ssh $instanceName --zone "$zone" -- "mongoimport --db=$database --collection=cities --type=csv --headerline --file=/tmp/regData/cities.csv --batchSize=50000 --numInsertionWorkers=4 --username $user --password $user_password --authenticationDatabase admin"
gcloud compute ssh $instanceName --zone "$zone" -- "mongoimport --db=$database --collection=airports --type=csv --headerline --file=/tmp/regData/airports_mongo.csv --batchSize=50000 --numInsertionWorkers=4 --username $user --password $user_password --authenticationDatabase admin"
