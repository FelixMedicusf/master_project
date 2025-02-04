#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

version=8
config_file="/etc/mongod.conf"
log_dir="/var/log/mongodb"
data_dir="/var/lib/mongodb"
conf_data_dir="/var/lib/mongoConfDB"

user="felix"
user_password="master"
database="aviation_data"

instanceName="mongodb-node-1"
zone="$(gcloud compute instances list --filter="name=$instanceName" --format "get(zone)" | awk -F/ '{print $NF}')"

echo "Provisioning $instanceName"

# Step 1: Install required dependencies and add MongoDB repo
gcloud compute ssh $instanceName --zone $zone -- "sudo apt-get update && sudo apt-get install -y gnupg curl"
gcloud compute ssh $instanceName --zone $zone -- "curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor"
gcloud compute ssh $instanceName --zone $zone -- "echo \"deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse\" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list && sudo apt-get update"

# Step 2: Install MongoDB and configure authentication
gcloud compute ssh $instanceName --zone $zone -- "sudo apt-get install -y mongodb-org"
gcloud compute ssh $instanceName --zone $zone -- "sudo chmod 644 $config_file && echo -e \"\nsecurity:\n  authorization: enabled\" | sudo tee -a /etc/mongod.conf"

# Step 3: Start MongoDB service
gcloud compute ssh $instanceName --zone $zone -- "sudo systemctl start mongod && sudo systemctl enable mongod"

# Step 4: Wait for MongoDB to be fully started
sleep 10

# Step 5: Create MongoDB root user securely
gcloud compute ssh $instanceName --zone $zone -- "echo \"use admin; db.createUser({ user: '$user', pwd: '$user_password', roles: [ { role: 'root', db: 'admin' } ] });\" | mongosh --host localhost"

# Step 6: Restart MongoDB to apply authentication settings
gcloud compute ssh $instanceName --zone $zone -- "sudo systemctl restart mongod"

echo "MongoDB setup completed on $instanceName with user '$user' and password '$user_password'."


gcloud compute ssh "$instanceName" --zone "$firstZone" -- "sudo apt install -y python3-pip && pip install gdown"

gcloud compute ssh "$instanceName" --zone "$firstZone" -- "echo 'downloading flightdata' && ~/.local/bin/gdown $flight_data_resource_id_large && sudo mv /home/felix/FlightPointsMobilityDBlarge.csv /tmp/FlightPointslarge.csv"

gcloud compute ssh $instanceName --zone $firstZone -- "echo 'downloading regional Data' && sudo mkdir /tmp/regData && cd /tmp/regData && sudo chmod 777 . && ~/.local/bin/gdown $cities_resource_id && ~/.local/bin/gdown $municipalities_resource_id && ~/.local/bin/gdown $counties_resource_id && ~/.local/bin/gdown $districts_resource_id &&  ~/.local/bin/gdown $airports_resource_id"

gcloud compute ssh $instanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"flightpoints\")'"
gcloud compute ssh $instanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"cities\")'"
gcloud compute ssh $instanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"airports\")'"
gcloud compute ssh $instanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"districts\")'"
gcloud compute ssh $instanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"counties\")'"
gcloud compute ssh $instanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'db.getSiblingDB(\"$database\").createCollection(\"municipalities\")'"

gcloud compute ssh $instanceName --zone $firstZone -- "mongosh -u $user -p $user_password --eval 'sh.enableSharding(\"$database\")'"


gcloud compute ssh $firstInstanceName --zone $firstZone -- "
mongosh -u $user -p $user_password --eval '
db.getSiblingDB(\"$database\").municipalities.createIndex({ name: \"hashed\" });
db.getSiblingDB(\"$database\").districts.createIndex({ name: \"hashed\" });
db.getSiblingDB(\"$database\").counties.createIndex({ name: \"hashed\" });
db.getSiblingDB(\"$database\").airports.createIndex({ ICAO: \"hashed\" });
db.getSiblingDB(\"$database\").cities.createIndex({ name: \"hashed\" });
'"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongoimport --db=$database --collection=flightpoints --type=csv --headerline --file=/tmp/FlightPointslarge.csv --batchSize=100000 --numInsertionWorkers=8 --username $user --password $user_password --authenticationDatabase admin"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongoimport --db=$database --collection=cities --type=csv --headerline --file=/tmp/regData/cities.csv --batchSize=50000 --numInsertionWorkers=4 --username $user --password $user_password --authenticationDatabase admin"
gcloud compute ssh $firstInstanceName --zone $firstZone -- "mongoimport --db=$database --collection=airports --type=csv --headerline --file=/tmp/regData/airports_mongo.csv --batchSize=50000 --numInsertionWorkers=4 --username $user --password $user_password --authenticationDatabase admin"
