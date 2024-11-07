#!/bin/bash


user="felix"
database="aviation_data"

db_host="localhost"
db_port="5432"

# Get number of SECONDO nodes
instances="$(gcloud compute instances list)"
value="secondo"
nodeNumber="$(echo -n $instances | grep -Fo $value | wc -l)"

instanceGroupName=${name:-secondo-node}

declare -a externalIpAdresses
declare -a internalIpAdresses

ssh_config=""

for (( i=1; i <= $nodeNumber; ++i ))
do 
    currentInstanceName="${instanceGroupName}-$i"
    zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"
    nodeExternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].accessConfigs[0].natIP)')"
    nodeInternalIp="$(gcloud compute instances describe $currentInstanceName --zone=$zone --format='get(networkInterfaces[0].networkIP)')"

    externalIpAdresses+=$nodeExternalIp
    internalIpAdresses+=$nodeInternalIp

    ssh_config+="Host ${currentInstanceName}\nHostName ${nodeInternalIp}\nUser felixbieleit\n"

done

queryProcessingNode="secondo-node-1"
storageNode="secondo-node-1"
storageNodeIp=${internalIpAdresses[0]}

if [[ $nodeNumber -eq 2 ]]; then

    queryProcessingNode="secondo-node-2"
    storageNode="secondo-node-2"
    storageNodeIp=${internalIpAdresses[1]}

fi

if [[ $nodeNumber -eq 3 ]]; then

    queryProcessingNode="secondo-node-2"
    storageNode="secondo-node-3"
    storageNodeIp=${internalIpAdresses[2]}

fi

# Loop through all deployed nodes to deploy SECONDO on them 
for (( i=1; i <= $nodeNumber; ++i ))
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

gcloud compute scp --zone $zone InstallSDK_Ubuntu_22_04.bash $currentInstanceName:~/secondoInstallScript.bash
gcloud compute ssh $currentInstanceName --zone $zone -- "sudo chmod +x ~/InstallSDK_Ubuntu_22_04.bash && bash ~/InstallSDK_Ubuntu_22_04.bash"
# gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get install -y cmake libtool automake git openssh-client screen build-essential screen libssl-dev libgsl-dev libgsl23"


if [[ $nodeNumber -eq 1]]; then
    gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"deb http://newton2.fernuni-hagen.de/secondo/download/repos/focalfossa ./\" | sudo tee -a /etc/apt/sources.list"
    gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get --allow-insecure-repositories update  && sudo apt-get install -y  --allow-unauthenticated secondo"
    gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"\" | bash /opt/secondo/bin/secondo_installer.sh"
    gcloud compute ssh $currentInstanceName --zone $zone -- "source .secondorc"
fi



if [[ $nodeNumber -gt 1 ]]; then


# install SECONDO management node
if [[ $i == 1]]; then

    # Install libraries for distributed SECONDO
    gcloud compute ssh $currentInstanceName --zone $zone -- "sudo apt-get install -y cmake libtool automake git openssh-client screen build-essential screen libssl-dev"
    gcloud compute ssh $currentInstanceName --zone $zone -- "export DSECONDO_DIR=~/dsecondo && mkdir -p $DSECONDO_DIR"
    
    # Copy script and install Secondo SDK
    gcloud compute scp --zone $zone InstallSDK_Ubuntu_22_04.bash $currentInstanceName:~/InstallSDK_Ubuntu_22_04.bash
    gcloud compute ssh $currentInstanceName --zone $zone -- "sudo chmod +x ~/InstallSDK_Ubuntu_22_04.bash && bash ~/InstallSDK_Ubuntu_22_04.bash"

    # Download Secondo
    gcloud compute ssh $currentInstanceName --zone $zone -- "cd $DSECONDO_DIR && sudo curl -O http://newton2.fernuni-hagen.de/secondo/download/secondo-RC_430-LAT1.tar.gz && sudo tar -xzf secondo-RC_430-LAT1.tar.gz -C \"$DSECONDO_DIR\""

    contentToAdd1=$(cat <<EOF
###
# DSECONDO
###

export DSECONDO_DIR=$DSECONDO_DIR
export DSECONDO_QPN_DIR=$DSECONDO_DIR
export SEC_DIR=$DSECONDO_DIR/secondo
export SECONDO_BUILD_DIR=$DSECONDO_DIR/secondo

# Locate the libuv and cpp-driver installation dir
if [ -d \$DSECONDO_DIR/driver/libuv ]; then
    export LD_LIBRARY_PATH=$DSECONDO_DIR/driver/libuv/.libs:\$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=$DSECONDO_DIR/driver/cpp-driver:\$LD_LIBRARY_PATH
else
    export LD_LIBRARY_PATH=$DSECONDO_QPN_DIR/driver/libuv/.libs:\$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=$DSECONDO_QPN_DIR/driver/cpp-driver:\$LD_LIBRARY_PATH
fi

# DSECONDO - Hostnames of the QPNs
export DSECONDO_QPN="$queryProcessingNode"

# DSECONDO - Hostnames of the SNs
export DSECONDO_SN="$storageNode"
EOF
)

    gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"$contentToAdd1\" >> ~/.secondorc"
    gcloud compute ssh $currentInstanceName --zone $zone -- "source ~/.secondorc"
    gcloud compute ssh $currentInstanceName --zone $zone -- "cd $SEC_DIR/Algebras/Cassandra/tools && ./manage_dsecondo.sh install_driver"

    contentToAdd2=$(cat <<EOF
ALGEBRA_DIRS += Cassandra
ALGEBRAS += CassandraAlgebra
ALGEBRA_DEPS += uv cassandra

ALGEBRA_INCLUDE_DIRS += $DSECONDO_DIR/driver/cpp-driver/include
ALGEBRA_INCLUDE_DIRS += $DSECONDO_DIR/driver/libuv/include

ALGEBRA_DEP_DIRS += $DSECONDO_DIR/driver/libuv/.libs
ALGEBRA_DEP_DIRS += $DSECONDO_DIR/driver/cpp-driver
EOF
)

    gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"$contentToAdd2\" | sudo tee \"$SEC_DIR/makefile.algebras\" > /dev/null"

    # build secondo 
    gcloud compute ssh $currentInstanceName --zone $zone -- "cd $SEC_DIR && sudo -E make"

    # SN needs to be replaced with the IP of one of the storage nodes
    contentToAdd3=$(cat <<EOF
[CassandraAlgebra]
CassandraHost=$storageNodeIp
CassandraKeyspace=keyspace_r3
CassandraConsistency=QUORUM
CassandraDefaultNodename=$queryProcessingNode
EOF
)

    
    gcloud compute ssh $currentInstanceName --zone $zone -- "echo \"$contentToAdd3\" | sudo tee \"$SEC_DIR/bin/SecondoConfig.ini\" > /dev/null"
    
    # ssh configuration on source
    gcloud compute ssh $currentInstanceName --zone $zone -- "ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa <<< ''$'\n'"
    rsa_pub_key=$(gcloud compute ssh $currentInstanceName --zone $zone -- "cat ~/.ssh/id_rsa.pub")
    gcloud compute ssh $currentInstanceName --zone $zone "echo \"$ssh_config\" > ~/.ssh/config"


fi

if [[ $i -gt 1 ]]; then

    # ssh configuration on target
    gcloud compute ssh $currentInstanceName --zone $zone "printf \"\n%s\n\" \"$rsa_pub_key\" >> ~/.ssh/authorized_keys"
    gcloud compute ssh $currentInstanceName --zone $zone "sudo chown -R felixbieleit:felixbieleit /opt/ && sudo chown -R felixbieleit:felixbieleit /mnt/"
fi

fi 

done


# Install query processing nodes using manager node
gcloud compute sssh $firstInstanceName --zone $zone -- "cd $SEC_DIR/Algebras/Cassandra/tools && ./manage_dsecondo.sh install"

# Install storage nodes using manager node
gcloud compute sssh $firstInstanceName --zone $firstZone -- "cd $SEC_DIR/Algebras/Cassandra/tools && ./manage_cassandra.sh install && ./manage_cassandra.sh start"
gcloud compute sssh $firstInstanceName --zone $firstZone -- "cd $SEC_DIR/Algebras/Cassandra/tools && ./manage_cassandra.sh init"



if [[ $nodeNumber -gt 1 ]]; then

    

fi

echo "Started first SECONDO node (${i}a) in ${currentInstanceName}"



if [[ $i -ne 1 ]]; then 

ipAdresses+=$nodeInternalIp

fi



# first started SECONDO node serves as a coordinator for distributed setup
if [[ $nodeNumber -gt 1 ]]; then

gcloud compute ssh $firstInstanceName --zone $firstZone -- "export PGPASSWORD=$user_password && psql -h $db_host -U $user -d $database -p $db_port -c \"SELECT citus_set_coordinator_host('$seedIp', 5432);\""

for ipAdress in "${ipAdresses[@]}"
    do

    done

fi