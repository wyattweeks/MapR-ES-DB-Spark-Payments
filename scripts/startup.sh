#  Streaming ETL Pipeline to Transform, Store and Explore Healthcare Dataset using Spark, JSON, MapR-DB, MapR-ES, Drill

## Introduction
This example will show you how to work with MapR-ES, Spark Streaming, and MapR-DB JSON :
* Publish using the Kafka API  Medicare Open payments data from a CSV file into MapR-ES 
* Consume and transform the streaming data with Spark Streaming and the Kafka API.
* Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL.
* Query the MapR-DB document database using Apache Drill. 
* Query the MapR-DB document database using Java and the OJAI library.
**Prerequisites**
* MapR Converged Data Platform 6.0 with Apache Spark and Apache Drill OR [MapR Container for Developers](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
* JDK 8
* Maven 3.x (and or IDE such as Netbeans or IntelliJ )

#!/bin/bash -x
#version date - 4/3/2018
#version id - 1
#mapr job custom script

echo "Running Custom MAPR cluster jobs ... version 2"

#Run custom job actions ( maprcli, ...)

MCS_HOST=${MAPR_CLDB_HOSTS:-mapr-cldb}
MCS_PORT=${MCS_PORT:-8443}
MCS_URL="https://${MCS_HOST}:${MCS_PORT}"
MAPR_ADMIN=${MAPR_ADMIN:-mapr}
MAPR_ADMIN_PASSWORD=${MAPR_ADMIN_PASSWORD:-maprmapr} 

chk_str="Waiting ..."

check_cluster(){
	if ! $(curl --output /dev/null -Iskf $MCS_URL); then
		chk_str="Waiting for MCS at $MCS_URL to start..."
		return 1
	fi

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


curl -s -u 'maprse:mapr$e4mapr!' 'http://stage.mapr.com/license/LatestDemoLicense-M7.txt' > license.txt

maprcli license add -license license.txt -is_file true

maprcli cluster gateway set -dstcluster ${MAPR_CLUSTER} -gateways mapr-gw

maprcli cluster queryservice setconfig -enabled true -clusterid ${MAPR_CLUSTER}-drillbits -storageplugin dfs -znode /drill

su mapr -c 'maprcli table create -tabletype json -path /user/mapr/ps -regionsizemb 256'

# su mapr -c 'hadoop fs -copyFromLocal /public_data/product_json/product_sales.json /user/mapr'

# su mapr -c 'mapr importJSON -src /user/mapr/product_sales.json -dst /user/mapr/ps'


#-----ABOVE is UNTESTED-------------------------------------------------------------------------------------------------

#-----START HERE

#### 0. Use REST to create volumes
# create volumes for files tables and streams
# test on sandbox
# curl -sSk -X POST -u 'mapr:mapr' "http://192.168.99.103:8443/rest/volume/create?name=files7&path=/user/files/&topology=/data&replication=3&type=rw"

# change to use variables:  curl -sSk -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} ${MCS_URL}/rest/volume/create?name=files&path=/var/mapr/local/demo.mapr.com/files&replication=3&topology=/data/default-rack&type=rw
curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/volume/create?name=demo.mapr.com&path=/user/mapr/demo.mapr.com/&topology=/data/default-rack&replication=3&type=rw"
curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/volume/create?name=files.demo.mapr.com&path=/user/mapr/demo.mapr.com/files/&topology=/data/default-rack&replication=3&type=rw"
curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/volume/create?name=tables.demo.mapr.com&path=/user/mapr/demo.mapr.com/tables/&topology=/data/default-rack&replication=3&type=rw"
curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/volume/create?name=streams.demo.mapr.com&path=/user/mapr/demo.mapr.com/streams/&topology=/data/default-rack&replication=3&type=rw"

#### 1. Create MapR-ES Stream, Topic, and MapR-DB table
# use REST to create streams and tables
# maprcli stream create -path /user/mapr/demo.mapr.com/streams/paystream -produceperm p -consumeperm p -topicperm p
curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/stream/create?path=/user/mapr/demo.mapr.com/streams/paystream&produceperm=p&consumeperm=p&topicperm=p"
# maprcli stream topic create -path /user/mapr/demo.mapr.com/streams/paystream -topic payments
curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/stream/topic/create?path=/user/mapr/demo.mapr.com/streams/paystream&topic=payments"
# maprcli table create -path /user/mapr/demo.mapr.com/tables/payments -tabletype json -defaultreadperm p -defaultwriteperm p
curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/table/create?path=/user/mapr/demo.mapr.com/tables/payments&tabletype=json&defaultreadperm=p&defaultwriteperm=p"

# need to install git and clone project (may need to do install 2x?) - do once on public_data, but may want to refresh and rebuild jars on startup
# if needed sudo apt-get -y install git
# on public_data - set wd
# git clone http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git
# sudo apt-get - y install maven 

# Create the following jars from git project
#`mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0.jar`
# `mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0-jar-with-dependencies.jar`
# on public data 
cd ~/MapR-ES-DB-Spark-Payments # change to public data
mvn clean install

# then mkdir ~/MapR-ES-DB-Spark-Payments
#copy jars from public data to 



##Run the java publisher and the Spark consumer**

#This client will read lines from the file in ./data/payments.csv and publish them to the topic /apps/paystream:payments. 
#You can optionally pass the file and topic as input parameters <file topic> 
java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer

###This spark streaming client will consume from the topic /apps/paystream:payments and write to the table /apps/payments.
You can wait for the java client to finish, or from a separate mac terminal you can run the spark streaming consumer with the following command, 
#or you can run from your IDE :
#You can optionally pass the topic and table as input parameters <topic table> 
cd /opt/mapr/spark/spark-2.2.1/bin
# use chads code to detect spark bin
#SPARK_VERSION=`apt-cache policy mapr-spark | grep Installed | awk '{print$2}' | cut -c 1-5`
#SPARK_PATH="/opt/mapr/opentsdb/opentsdb-$SPARK_VERSION"
#SPARK_VERSION=`apt-cache policy mapr-spark | grep Installed | awk '{print$2}' | cut -c 1-5`
#SPARK_PATH="/opt/mapr/spark/spark-$SPARK_VERSION"
#./$SPARK_PATH/bin/spark-submit
#./spark-submit --class streaming.SparkKafkaConsumer --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar
./spark-submit --class streaming.SparkKafkaConsumer --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar

