#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

pg_version=16
pg_config_file="/etc/postgresql/$pg_version/main/postgresql.conf"
pg_hba_file="/etc/postgresql/$pg_version/main/pg_hba.conf"
pg_pass_file="$HOME/.pgpass"

CONFIG="
# Memory settings
shared_buffers = '12GB'
work_mem = '248MB'
maintenance_work_mem = '2048MB'
effective_cache_size = '24GB'

# Parallel processing
max_parallel_workers_per_gather = 4
max_worker_processes = 16
max_parallel_workers = 16
max_parallel_maintenance_workers = 4

# I/O settings
effective_io_concurrency = 300

# WAL settings
archive_mode = off
max_wal_size = '10GB'
min_wal_size = '2GB'

# Autovacuum settings
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = '1min'
autovacuum_vacuum_threshold = 50
autovacuum_analyze_threshold = 50

# Citus settings (if applicable)
citus.max_parallel_workers_per_query = 6
citus.task_scheduler_slots = 8
citus.max_background_task_executors_per_node = 6
"

flight_data_resource_id="1REu74vRj6tsoPKO7J7bfOEjjdaWEY4Dm"
flight_data_resource_id_large="1Q7Yio3eUulzE6jl8J1zkWJiLqrHOzSUv"
cities_resource_id="1KPNtXMNCAIH2wgGeWQYnCbBfakhlOJz5"
municipalities_resource_id="1IxS8b4RaNe9glfdk4ZurXrZiZFnJNhC5"
counties_resource_id="1KkNU4iwMeIHDoBMhFI4eJkruTFlm3xAP"
districts_resource_id="1psjLKgaciSXmVBs-DITOPHNE5UMfHmss"
airports_resource_id="1k1NcL5XOFpMz0jX-KKl0_PjF-4ReOtYH"

user="felix"
user_password="master"
database="aviation_data"

db_host="localhost"
db_port="5432"

# Get number of MobilityDB nodes
instances="$(gcloud compute instances list)"
value="mobilitydb"
nodeNumber="$(echo -n $instances | grep -Fo $value | wc -l)"

instanceGroupName=${name:-mobilitydb-node}

declare -a ipAdresses

# Loop through all deployed nodes to provision them Postgresql instances
for (( i=1; i <= nodeNumber; ++i ))
do 
firstInstanceName="${instanceGroupName}-1"
currentInstanceName="${instanceGroupName}-$i"
zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"

echo "Provisioning $currentInstanceName"


if [[ $i -eq 1 ]];then

firstZone=$zone
seedIp=$nodeInternalIp
seedIpExternal=$nodeExternalIp
firstInstanceName=$currentInstanceName

fi

# Install PostgreSQL
gcloud compute ssh $currentInstanceName --zone $zone -- \
  "sudo apt install -y gnupg2 && \
   sudo sh -c 'echo \"deb http://apt.postgresql.org/pub/repos/apt/ \$(lsb_release -cs)-pgdg main\" > /etc/apt/sources.list.d/pgdg.list'"

gcloud compute ssh $currentInstanceName --zone $zone -- "curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg && sudo apt update"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt install -y postgresql-$pg_version postgresql-contrib-$pg_version"

# configuring the postgresql database to allow external connections
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"s/#listen_addresses = 'localhost'/listen_addresses = '*'/g\" $pg_config_file"

# PostGIS installation
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt install -y postgresql-$pg_version-postgis-3"

# MobilityDB Installation 
gcloud compute ssh $currentInstanceName --zone $zone -- "git clone https://github.com/MobilityDB/MobilityDB"
gcloud compute ssh $currentInstanceName --zone $zone -- "mkdir MobilityDB/build && cd MobilityDB/build && sudo apt install -y cmake && sudo apt install -y g++"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt install -y libgeos-dev && sudo apt install -y libproj-dev && sudo apt install -y libjson-c-dev && sudo apt install -y libgsl-dev && sudo apt install -y libpq-dev"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get -y install postgresql-server-dev-$pg_version"
gcloud compute ssh $currentInstanceName --zone $zone -- "cd MobilityDB/build || true && cmake .. && make && sudo make install"

# config changes for mobilitydb
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"/^#shared_preload_libraries/c\shared_preload_libraries = 'postgis-3'\" $pg_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"/^#max_locks_per_transaction/c\max_locks_per_transaction = 128\" $pg_config_file"

# restart database
gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"$db_host:5432:*:$user:$user_password\" > \"$pg_pass_file\" && chmod 600 \"$pg_pass_file\""
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo systemctl restart postgresql"

gcloud compute ssh $currentInstanceName --zone $zone -- "export PGPASSWORD=$user_password"

# Create user and database
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo -u postgres psql -c \"CREATE USER $user WITH PASSWORD '$user_password' SUPERUSER;\""
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo -u postgres psql -c \"CREATE DATABASE $database OWNER $user;\""

# Create PostGIS and MobilityDB extensions
gcloud compute ssh $currentInstanceName --zone $zone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"CREATE EXTENSION IF NOT EXISTS postgis;\""
gcloud compute ssh $currentInstanceName --zone $zone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"CREATE EXTENSION IF NOT EXISTS MobilityDB;\""



if [[ $nodeNumber -gt 1 ]]; then
# Add Citus 
gcloud compute ssh $currentInstanceName --zone $zone -- "curl https://install.citusdata.com/community/deb.sh | sudo bash"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get -y install postgresql-$pg_version-citus-12.1" 

# config changes for citus 
# gcloud compute ssh $currentInstanceName --zone $zone -- "sudo pg_conftool 16 main set shared_preload_libraries citus"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i \"/^shared_preload_libraries/c\shared_preload_libraries = 'citus,postgis-3'\" $pg_config_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i '\$a\host    all             all             10.0.0.0/8              trust' $pg_hba_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i '\$a\host    all             all             127.0.0.1/32            trust' $pg_hba_file"
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i '\$a\host    all             all             ::1/128                 trust' $pg_hba_file"

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo systemctl restart postgresql"

# create extension for citus for the database
gcloud compute ssh $currentInstanceName --zone $zone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"CREATE EXTENSION IF NOT EXISTS citus;\""

fi

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sed -i '\$a\host    all             all             0.0.0.0/0               md5' $pg_hba_file"

gcloud compute ssh $currentInstanceName --zone $zone -- "sudo sysctl -w kernel.shmmax=2147483648 && sudo sysctl -w kernel.shmall=524288"
gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"$CONFIG\" | sudo tee -a $pg_config_file"


gcloud compute ssh $currentInstanceName --zone $zone -- "sudo systemctl restart postgresql"

echo "Started mobiltyDB node (${i}) in ${currentInstanceName}"

ipAdresses[i]=$nodeInternalIp

done

# first started mobility node serves as a coordinator for distributed setup
if [[ $nodeNumber -gt 1 ]]; then

gcloud compute ssh $firstInstanceName --zone $firstZone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"SELECT citus_set_coordinator_host('$seedIp', 5432);\""

for ipAdress in "${ipAdresses[@]}"

do
gcloud compute ssh $firstInstanceName --zone $firstZone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"SELECT * from citus_add_node('$ipAdress', 5432);\""

done

fi

# echo "username ALL=(ALL) ALL" >> /etc/sudoers oder sudo usermod -aG sudo felixbieleit

gcloud compute ssh $firstInstanceName --zone $firstZone -- "sudo apt install -y python3-pip && pip install gdown"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "echo 'downloading flightdata' && ~/.local/bin/gdown $flight_data_resource_id_large && sudo mv /home/felix/FlightPointsMobilityDBlarge.csv /tmp/FlightPointsMobilityDBlarge.csv"

gcloud compute ssh $firstInstanceName --zone $firstZone -- "echo 'downloading regional Data' && sudo mkdir /tmp/regData && cd /tmp/regData && sudo chmod 777 . && ~/.local/bin/gdown $cities_resource_id && ~/.local/bin/gdown $municipalities_resource_id && ~/.local/bin/gdown $counties_resource_id && ~/.local/bin/gdown $districts_resource_id &&  ~/.local/bin/gdown $airports_resource_id"

