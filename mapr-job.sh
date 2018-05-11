#  Streaming ETL Pipeline to Transform, Store and Explore Healthcare Dataset using Spark, JSON, MapR-DB, MapR-ES, Drill
#  Carol McDonald's Blog Post: (https://mapr.com/blog/streaming-data-pipeline-transform-store-explore-healthcare-dataset-mapr-db/)
#  Cloned from Carol McDonald's repository https://github.com/mapr-demos/mapr-es-db-spark-payment, and adapted for SE Cluster @ SE private git http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git 
#  stored locally on the SE Demo Cluster @ /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments

## Introduction 
#This example will show you how to work with MapR-ES, Spark Streaming, and MapR-DB JSON :
#* Publish using the Kafka API  Medicare Open payments data from a CSV file into MapR-ES 
#* Consume and transform the streaming data with Spark Streaming and the Kafka API.
#* Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
#* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL.
#* Query the MapR-DB document database using Apache Drill. 
#* Query the MapR-DB document database using Java and the OJAI library.
#**Prerequisites**
#* MapR Converged Data Platform 6.0 with Apache Spark and Apache Drill OR [MapR Container for Developers](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
#* JDK 8
#* Maven 3.x (and or IDE such as Netbeans or IntelliJ )

#!/bin/bash -x
#version date - 5/4/18
#version id - 1

echo "Running Custom MAPR Demo setup for /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments version 1"

#Run custom job actions

###set environment variables in ~/.profile
MCS_HOST=${MAPR_CLDB_HOSTS:-cldb}
MCS_PORT=${MCS_PORT:-8443}
MCS_URL="https://${MCS_HOST}:${MCS_PORT}"
MAPR_ADMIN=${MAPR_ADMIN:-mapr}
MAPR_ADMIN_PASSWORD=${MAPR_ADMIN_PASSWORD:-maprmapr}
echo MCS_URL=$MCS_URL >> /etc/profile
echo MAPR_ADMIN=$MAPR_ADMIN >> /etc/profile
echo MAPR_ADMIN_PASSWORD=$MAPR_ADMIN_PASSWORD >> /etc/profile
echo MAPR_CLUSTER=$MAPR_CLUSTER >> /etc/profile

###set spark path and version to future-proof
SPARK_VERSION=`apt-cache policy mapr-spark | grep Installed | awk '{print$2}' | cut -c 1-5`
SPARK_PATH="/opt/mapr/spark/spark-$SPARK_VERSION"
echo SPARK_PATH=$SPARK_PATH >> /etc/profile

###wait for cluster to come up before executing mapr jobs
echo ""
echo "MapR JOB script executing for demo"
echo ""
chk_str="Waiting ..."
# Check that the CLDB is up and running.
check_cluster(){
        find_cldb="curl -sSk -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} ${MCS_URL}/rest/node/cldbmaster"
        if [ "$($find_cldb | jq -r '.status')" = "OK" ]; then
                return 0
        else
                echo "Connected to $MCS_URL, Waiting for CLDB Master to be Ready..."
                return 1
        fi
}
until check_cluster; do
    echo "$chk_str"
    sleep 10
done
echo "CLDB Master is ready, continuing startup for $MAPR_CLUSTER ..."

#### setup demo on edge node
## Use REST to create volumes, tables, streams
su - mapr
# create volumes 
curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/volume/create?name=demo.mapr.com&path=/user/mapr/demo.mapr.com/&topology=/data/default-rack&replication=3&type=rw"
curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/volume/create?name=files.demo.mapr.com&path=/user/mapr/demo.mapr.com/files/&topology=/data/default-rack&replication=3&type=rw"
curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/volume/create?name=tables.demo.mapr.com&path=/user/mapr/demo.mapr.com/tables/&topology=/data/default-rack&replication=3&type=rw"
curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/volume/create?name=streams.demo.mapr.com&path=/user/mapr/demo.mapr.com/streams/&topology=/data/default-rack&replication=3&type=rw"

# Create MapR-ES Stream, Topic, and MapR-DB table via REST APIs
curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/stream/create?path=/user/mapr/demo.mapr.com/streams/paystream&produceperm=p&consumeperm=p&topicperm=p"
curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/stream/topic/create?path=/user/mapr/demo.mapr.com/streams/paystream&topic=payments"
curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/table/create?path=/user/mapr/demo.mapr.com/tables/payments&tabletype=json&defaultreadperm=p&defaultwriteperm=p"

#copy MapR-ES-DB-Spark-Payments data source file payments.csv, from public_data to the MapR 'files' volume
cp /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/data/payments.csv /mapr/$MAPR_CLUSTER/user/mapr/demo.mapr.com/files/payments.csv

# Run the java publisher client and the Spark consumer client**
# This java client will read lines from the file in ~/MapR-ES-DB-Spark-Payments/data/payments.csv and publish them to the topic /streams/paystream:payments. 
# UNCOMMENT BELOW TO AUTO-START THE PRODUCER ON DEPLOYMENT - Currently, the demo is setup for the SE to start the producer manually - see readme
# java -cp ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer

# Create Drill views on the MapR-DB payments table, for use with queries and Tableau Desktop reports that connect to MapR-DB using Drill
sqlline
use dfs.tmp;
create or replace view physicians_by_revenue as select physician_id, sum(amount) as revenue from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_id;
create or replace view physicians_by_specialty_revenue as select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty;
create or replace view aca_open_payments as select recipient_country, recipient_state, physician_specialty, recipient_zip, payer, nature_of_payment, (sum(amount)) as us_dollars from dfs.`/user/mapr/demo.mapr.com/tables/payments` GROUP BY recipient_country, recipient_state, recipient_zip, physician_specialty, payer, nature_of_payment;
!quit