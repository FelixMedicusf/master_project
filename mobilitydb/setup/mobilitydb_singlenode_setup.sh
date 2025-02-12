#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

pg_version=16
pg_config_file="/etc/postgresql/$pg_version/main/postgresql.conf"
pg_hba_file="/etc/postgresql/$pg_version/main/pg_hba.conf"
pg_pass_file="$HOME/.pgpass"

CONFIG="
# Memory settings
shared_buffers = '8GB'
work_mem = '500MB'
maintenance_work_mem = '2048MB'
effective_cache_size = '24GB'

# Parallel processing
max_parallel_workers_per_gather = 4
max_worker_processes = 8
max_parallel_workers = 8
max_parallel_maintenance_workers = 4

# I/O settings
effective_io_concurrency = 200

# WAL settings
archive_mode = off
max_wal_size = '10GB'
min_wal_size = '2GB'

# Autovacuum settings
autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = '1min'
autovacuum_vacuum_threshold = 5000
autovacuum_analyze_threshold = 5000

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


instanceName="mobilitydb-node-1"
zone="$(gcloud compute instances list --filter="name=$instanceName" --format "get(zone)" | awk -F/ '{print $NF}')"


# Install PostgreSQL
gcloud compute ssh $instanceName --zone $zone -- \
  "sudo apt install -y gnupg2 && \
   sudo sh -c 'echo \"deb http://apt.postgresql.org/pub/repos/apt/ \$(lsb_release -cs)-pgdg main\" > /etc/apt/sources.list.d/pgdg.list'"

gcloud compute ssh $instanceName --zone $zone -- "curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg && sudo apt update"
gcloud compute ssh $instanceName --zone $zone -- "sudo apt install -y postgresql-$pg_version postgresql-contrib-$pg_version"

# configuring the postgresql database to allow external connections
gcloud compute ssh $instanceName --zone $zone -- "sudo sed -i \"s/#listen_addresses = 'localhost'/listen_addresses = '*'/g\" $pg_config_file"

# PostGIS installation
gcloud compute ssh $instanceName --zone $zone -- "sudo apt install -y postgresql-$pg_version-postgis-3"

# MobilityDB Installation 
gcloud compute ssh $instanceName --zone $zone -- "git clone https://github.com/MobilityDB/MobilityDB"
gcloud compute ssh $instanceName --zone $zone -- "mkdir MobilityDB/build && cd MobilityDB/build && sudo apt install -y cmake && sudo apt install -y g++"
gcloud compute ssh $instanceName --zone $zone -- "sudo apt install -y libgeos-dev && sudo apt install -y libproj-dev && sudo apt install -y libjson-c-dev && sudo apt install -y libgsl-dev && sudo apt install -y libpq-dev"
gcloud compute ssh $instanceName --zone $zone -- "sudo apt-get -y install postgresql-server-dev-$pg_version"
gcloud compute ssh $instanceName --zone $zone -- "cd MobilityDB/build || true && cmake .. && make && sudo make install"

# config changes for mobilitydb
gcloud compute ssh $instanceName --zone $zone -- "sudo sed -i \"/^#shared_preload_libraries/c\shared_preload_libraries = 'postgis-3'\" $pg_config_file"
gcloud compute ssh $instanceName --zone $zone -- "sudo sed -i \"/^#max_locks_per_transaction/c\max_locks_per_transaction = 128\" $pg_config_file"

# restart database
gcloud compute ssh $instanceName --zone $zone -- "echo \"$db_host:5432:*:$user:$user_password\" > \"$pg_pass_file\" && chmod 600 \"$pg_pass_file\""
gcloud compute ssh $instanceName --zone $zone -- "sudo systemctl restart postgresql"

gcloud compute ssh $instanceName --zone $zone -- "export PGPASSWORD=$user_password"

# Create user and database
gcloud compute ssh $instanceName --zone $zone -- "sudo -u postgres psql -c \"CREATE USER $user WITH PASSWORD '$user_password' SUPERUSER;\""
gcloud compute ssh $instanceName --zone $zone -- "sudo -u postgres psql -c \"CREATE DATABASE $database OWNER $user;\""

# Create PostGIS and MobilityDB extensions
gcloud compute ssh $instanceName --zone $zone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"CREATE EXTENSION IF NOT EXISTS postgis;\""
gcloud compute ssh $instanceName --zone $zone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"CREATE EXTENSION IF NOT EXISTS MobilityDB;\""

gcloud compute ssh $instanceName --zone $zone -- "sudo sysctl -w kernel.shmmax=2147483648 && sudo sysctl -w kernel.shmall=524288"
gcloud compute ssh $instanceName --zone $zone -- "echo \"$CONFIG\" | sudo tee -a $pg_config_file"
gcloud compute ssh $instanceName --zone $zone -- "sudo sed -i '\$a\host    all             all             0.0.0.0/0               md5' $pg_hba_file"
gcloud compute ssh $instanceName --zone $zone -- "sudo systemctl restart postgresql"

echo "Started mobiltyDB node on ${instanceName}"

gcloud compute ssh $instanceName --zone $zone -- "sudo apt install -y python3-pip && pip install gdown"

gcloud compute ssh $instanceName --zone $zone -- "echo 'downloading flightdata' && ~/.local/bin/gdown $flight_data_resource_id_large && sudo mv /home/felix/FlightPointsMobilityDBlarge.csv /tmp/FlightPointsMobilityDBlarge.csv"

gcloud compute ssh $instanceName --zone $zone -- "echo 'downloading regional Data' && sudo mkdir /tmp/regData && cd /tmp/regData && sudo chmod 777 . && ~/.local/bin/gdown $cities_resource_id && ~/.local/bin/gdown $municipalities_resource_id && ~/.local/bin/gdown $counties_resource_id && ~/.local/bin/gdown $districts_resource_id &&  ~/.local/bin/gdown $airports_resource_id"