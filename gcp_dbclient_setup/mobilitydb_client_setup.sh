#!/bin/bash
export DEBIAN_FRONTEND=noninteractive

JAR_DEST_MOBILITYDB="/opt/mobilitydbBenchmarkingApplication.jar"

# Get number of Bench Client nodes
instances="$(gcloud compute instances list)"
value="benchclient"
nodeNumber="$(echo -n $instances | grep -Fo $value | wc -l)"

instanceGroupName=${name:-benchclient-node}


# Loop through all deployed benchmarking client machines and start BenchmarkingClientApplication for MobilityDB
for (( i=1; i <= nodeNumber; ++i ))
do 

    currentInstanceName="${instanceGroupName}-$i"
    zone="$(gcloud compute instances list --filter="name=$currentInstanceName" --format "get(zone)" | awk -F/ '{print $NF}')"

    echo "Start Client on $currentInstanceName"
    gcloud compute ssh $currentInstanceName --zone $zone -- "sudo java -Xms2g -Xmx24g -jar $JAR_DEST_MOBILITYDB"

done