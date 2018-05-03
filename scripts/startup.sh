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
# if needed sudo apt-get install -y git
# on public_data - set wd
# git clone http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git
# sudo apt-get install -y maven 

# Create the following jars from git project
#`mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0.jar`
# `mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0-jar-with-dependencies.jar`
# on public data 
cd ~/MapR-ES-DB-Spark-Payments # change to public data
mvn clean install

# then mkdir ~/MapR-ES-DB-Spark-Payments
# create new directory on edge node ~/MapR-ES-DB-Spark-Payments
mkdir
#copy MapR-ES-DB-Spark-Payments folder form public_data to ~/MapR-ES-DB-Spark-Payments
cp
#copy payments.csv from public_data - /MapR-ES-DB-Spark-Payments/data to 'files' volume (for show only - csv in ~/MapR-ES-DB-Spark-Payments will be used for producer)
cp 


##Run the java publisher and the Spark consumer**
#This client will read lines from the file in ~/MapR-ES-DB-Spark-Payments/data/payments.csv and publish them to the topic /streams/paystream:payments. 
#You can optionally pass the file and topic as input parameters <file topic> 
java -cp ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer

###This spark streaming client will consume from the topic /streams/paystream:payments and write to the table /tables/payments.
# You can wait for the java client to finish, or from a separate terminal you can run the spark streaming consumer with the following command
#You can optionally pass the topic and table as input parameters <topic table> 

# use chads code to detect spark bin to point to spark submit script loc
#SPARK_VERSION=`apt-cache policy mapr-spark | grep Installed | awk '{print$2}' | cut -c 1-5`
#SPARK_PATH="/opt/mapr/opentsdb/opentsdb-$SPARK_VERSION"
#SPARK_VERSION=`apt-cache policy mapr-spark | grep Installed | awk '{print$2}' | cut -c 1-5`
#SPARK_PATH="/opt/mapr/spark/spark-$SPARK_VERSION"
#./$SPARK_PATH/bin/spark-submit
#./spark-submit --class streaming.SparkKafkaConsumer --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar
cd /opt/mapr/spark/spark-2.2.1/bin
./spark-submit --class streaming.SparkKafkaConsumer --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


#### 3. Run the Spark SQL client to load and query data from MapR-DB with Spark-DB Connector

#You can wait for the java client and spark consumer to finish, or from a separate mac terminal you can run the spark sql with the following command, 
#or you can run from your IDE :
cd /opt/mapr/spark/spark-2.2.1/bin
./spark-submit --class sparkmaprdb.QueryPayment --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar

# this part is an option to the step above
# in a sparate terminal window, start the spark shell with this command
# add paramenter to spark version
# need to automate the 'copy paste step below'
$ /opt/mapr/spark/spark-2.2.1/bin/spark-shell --master local[2]
copy paste  from the scripts/sparkshell file to query MapR-DB

#### 4. Working with Drill-JDBC

#From your mac in the mapr-es-db-spark-payment directory you can run the java client to query the MapR-DB table using Drill-JDBC 
#or you can run from your IDE :
# WW -changed to run in dsr-demo
$ java -cp ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar:~/MapR-ES-DB-Spark-Payments/target/* maprdb.DRILL_SimpleQuery
#

#### 5. Working with OJAI

OJAI the Java API used to access MapR-DB JSON, leverages the same query engine as MapR-DB Shell and Apache Drill. 

From your mac in the mapr-es-db-spark-payment directory you can run the java client to query the MapR-DB table using OJAI
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* maprdb.OJAI_SimpleQuery
```

#### 6. Using the MapR-DB shell and Drill from your mac client 
Refer to [**connecting clients **](https://maprdocs.mapr.com/home/MapRContainerDevelopers/ConnectingClients.html) for 
information on setting up the Drill client

**Use MapR DB Shell to Query the Payments table**

In this section you will  use the DB shell to query the Payments JSON table

To access MapR-DB from your mac client or logged into the container, you can use MapR-DB shell:

```
$ /opt/mapr/bin/mapr dbshell
```

To learn more about the various commands, run help or help <command> , for example help insert.

```
$ maprdb mapr:> jsonoptions --pretty true --withtags false
```
**find 5 documents**
```
maprdb mapr:> find /apps/payments --limit 5
```
Note that queries by _id will be faster because _id is the primary index

**Query document with Condition _id starts with 98485 (physician id)**
```
maprdb mapr:> find /apps/payments --where '{ "$like" : {"_id":"98485%"} }' --f _id,amount
```
**Query document with Condition _id has february**
```
find /apps/payments --where '{ "$like" : {"_id":"%_02/%"} }' --f _id,amount
```
**find all payers=**
```
maprdb mapr:> find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
```

#  Use Drill Shell to query MapR-DB  
#From an edge node terminal, connect to Drill as user mapr through JDBC by running sqlline:
# this is CM's original cmd: /opt/mapr/drill/drill-1.11.0/bin/sqlline -u "jdbc:drill:drillbit=localhost" -n mapr
# WW - for dsr-demo, on edge node just: 
sqlline
# 0: jdbc:drill:zk=mapr-zk:5181/drill/dsr-demo->
# WW - changed these to work with dsr-demo
# 1 - Top 5 Physician Ids by Amount**
select physician_id, sum(amount) as revenue from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_id order by revenue desc limit 5;
# 2 - Top 5 nature of payments by Amount**
select nature_of_payment,  sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by nature_of_payment order by total desc limit 5;
# 3 - Query for payments for physician id**
select _id,  amount from dfs.`/user/mapr/demo.mapr.com/tables/payments` where _id like '98485%';
# 4 - Query for payments in february**
select _id,  amount from dfs.`/user/mapr/demo.mapr.com/tables/payments` where _id like '%[_]02/%';
# 5 - Queries on payer**
select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer='CorMatrix Cardiovascular Inc.';
select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer like '%Dental%';
select  distinct(payer) from dfs.`/user/mapr/demo.mapr.com/tables/payments`;

#### 7. Adding a secondary index to the payments JSON table, to speed up queries
# Let's now add indices to the payments table.
# In a terminal window:
need to convert this to REST
$ maprcli table index add -path /apps/payments -index idx_payer -indexedfields 'payer:1'

In MapR-DB Shell, try queries on payments payers and compare with previous query performance:
```
maprdb mapr:> find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
```
In Drill try 
```
0: jdbc:drill:drillbit=localhost> select _id, amount, payer from dfs.`/apps/payments` where payer='CorMatrix Cardiovascular Inc.';

0: jdbc:drill:drillbit=localhost> select _id, amount, payer from dfs.`/apps/payments` where payer like '%Dental%';

0: jdbc:drill:drillbit=localhost> select  distinct(payer) from dfs.`/apps/payments` ;

```
##Cleaning Up

You can delete the topic and table using the following command from a container terminal:
```
maprcli stream topic delete -path /mapr/maprdemo.mapr.io/apps/paystream -topic payment
maprcli table delete -path /mapr/maprdemo.mapr.io/apps/payments

```
## Conclusion

In this example you have learned how to:

* Publish using the Kafka API  Medicare Open payments data from a CSV file into MapR-ES 
* Consume and transform the streaming data with Spark Streaming and the Kafka API
* Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector
* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL 
* Query the MapR-DB document database using Apache Drill 
* Query the MapR-DB document database using Java and the OJAI library



You can also look at the following examples:

* [mapr-db-60-getting-started](https://github.com/mapr-demos/mapr-db-60-getting-started) to learn Discover how to use DB Shell, Drill and OJAI to query and update documents, but also how to use indexes.
* [Ojai 2.0 Examples](https://github.com/mapr-demos/ojai-2-examples) to learn more about OJAI 2.0 features
* [MapR-DB Change Data Capture](https://github.com/mapr-demos/mapr-db-cdc-sample) to capture database events such as insert, update, delete and react to this events.


