####  Streaming ETL Pipeline to Transform, Store and Explore Healthcare Dataset using Spark, JSON, MapR-DB, MapR-ES, Drill

# MapR-ES-DB-Spark-Payments project has been cloned to /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments (source = git clone http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git)
# Manually refresh when repo changes (see steps below)
# maven rebuilds jars in /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target
# (set auto refresh in future?)

# STEPS TO MANUALLY REFRESH FROM REPO
# IMPORTANT - when represhing repo, do this BEFORE deploying the MapR-ES-DB-Spark-Payments demo cluster, because jars for demo are copied from public data at cluster startup
# 1 - ssh to edge node of any cluster deployment as mapr
# 2 - $ cd /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments
# 3 - $ git pull 
# 4 - $ mvn clean install

### Introduction

## This example will show you how to work with MapR-ES, Spark Streaming, and MapR-DB JSON :
# 1 - Publish using the Kafka API Medicare Open payments data from a CSV file into MapR-ES 
# 2 - Consume and transform the streaming data with Spark Streaming and the Kafka API, and
#     Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
# 3 - Load data into Spark Dataset: Query the MapR-DB JSON document database with Spark-SQL, using the Spark-DB connector
# 4 - Query the MapR-DB document database using Apache Drill. 
# 5 - Query the MapR-DB document database using Java and the OJAI library.


### 
DEMO: STEP-BY-STEP
###

## 0 - Complete demo setup on edge node and cluster
# 
# ssh to edge node as mapr, and 
cd /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/scripts
chmod 667 demosetup.sh
./demosetup.sh


## 1 - Publish using the Kafka API Medicare Open payments data from a CSV file into MapR-ES 
#
# This java publisher client will read lines from the payments.csv and publish them in the same format (comma-delimited strings)
# to the MapR Stream:topic @ /streams/paystream:payments
#
# The paystream:payments stream:topic can be viewed in MCS @ path /mapr/dsr-demo/user/mapr/demo.mapr.com/streams/paystream
#
# THIS CLIENT PROCESS IS STARTED AUTOMATICALLY during cluster deployment
#
# For presentation purposes, you may start a second producer client manually, from a separate terminal window
# ssh to the cluster edge node as mapr and:
#
cd ~/MapR-ES-DB-Spark-Payments
java -cp ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer


### 2 - Consume and transform the streaming data with Spark Streaming and the Kafka API, and
#       Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
#
# This Spark Streaming consumer client does the following:
#      - consumes data from the MapR stream:topic @ /streams/paystream:payments using the Kafka API, then
#      - transforms the comma-delimited string consumed from the stream, into JSON format, then
#      - writes the JSON array, to the MapR-DB JSON table 'payments' using the Spark-DB connector.
# 
# The 'payments' table can be viewed in MCS @ path /user/mapr/demo.mapr.com/tables/payments
#
# THIS CLIENT PROCESS IS STARTED AUTOMATICALLY during cluster deployment
#
# For presentation purposes, you may start a second consumer client manually, from a separate terminal window
# Note: you'll see the client looking for data in the stream, but unless you stop the auto-deployed consumer, it won't actually consume data from the stream,
#      as the original consumer client is capable of consuming all the data being produced.  OR the original consumer client may fail (due to a conflict in reading 
#      from the stream partition), in which case, this newly launched consumer will pick up where the other left off) See future enhancement on this.
# Note: if spark version changes, change path in 'cd' cmd below
# ssh to the cluster edge node as mapr and:
#
cd $SPARK_PATH/bin
./spark-submit --class streaming.SparkKafkaConsumer --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


### 3 - Load data into Spark Dataset: Query the MapR-DB JSON document database with Spark-SQL, using the Spark-DB connector
#
# ssh to the cluster edge node as mapr and:
# Note: if spark version changes, change path in 'cd' cmd below
#
cd $SPARK_PATH/bin
./spark-submit --class sparkmaprdb.QueryPayment --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


### 4 - Query the MapR-DB document database using Apache Drill (JDBC). 
#
# Show the Top 10 physician specialties by total payments
# Query = select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty order by total desc limit 10
#
cd $SPARK_PATH/bin
./spark-submit --class maprdb.DRILL_SimpleQuery --master local[2] --jars /opt/mapr/drill/jars/jdbc-driver/drill-jdbc-all-1.11.0.jar ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


### 5 - Query the MapR-DB document database using Java and the OJAI library.
# OJAI, the Java API used to access MapR-DB JSON, leverages the same query engine as MapR-DB Shell and Apache Drill to query payments table
# Query the MapR-DB payments table using OJAI
# original java version (old) $ java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* maprdb.OJAI_SimpleQuery
cd $SPARK_PATH/bin
./spark-submit --class maprdb.OJAI_SimpleQuery --master local[2] --jars /opt/mapr/drill/jars/jdbc-driver/drill-jdbc-all-1.11.0.jar ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


#### 6. Using the MapR-DB shell and Drill from your mac client 
#Refer to [**connecting clients **](https://maprdocs.mapr.com/home/MapRContainerDevelopers/ConnectingClients.html) for 
#information on setting up the Drill client
#**Use MapR DB Shell to Query the Payments table**
#In this section you will  use the DB shell to query the Payments JSON table
#To access MapR-DB from your mac client or logged into the container, you can use MapR-DB shell:
$ /opt/mapr/bin/mapr dbshell
#To learn more about the various commands, run help or help <command> , for example help insert.
$ maprdb mapr:> jsonoptions --pretty true --withtags false
#**find 5 documents**
maprdb mapr:> find /user/mapr/demo.mapr.com/tables/payments --limit 5
#Note that queries by _id will be faster because _id is the primary index
#Query document with Condition _id starts with 98485 (physician id)**
maprdb mapr:> find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"98485%"} }' --f _id,amount
#**Query document with Condition _id has february**
maprdb mapr:> find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"%_02/%"} }' --f _id,amount
#**find all payers=**
maprdb mapr:> find /user/mapr/demo.mapr.com/tables/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
#ctrl-C to exit maprdb shell


# WW - FUTURE enhancement - add Drill query on Mapr-ES

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
#!q to exit drill shell


#### 7. Adding a secondary index to the payments JSON table, to speed up queries
# Let's now add indices to the payments table.
# before performance:
$ /opt/mapr/bin/mapr dbshell
maprdb mapr:> find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
maprdb mapr:> ctrl-c

# In a terminal window create index
#need to convert this to REST
#$ maprcli table index add -path /apps/payments -index idx_payer -indexedfields 'payer:1'
#curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/table/index/add?path=/user/mapr/demo.mapr.com/tables/payments&index=idx_payer&indexedfields=payer:1"
#ERROR:  mapr@edge-5d5bbc5544-sbgs4:~$ curl -sSk -X POST -u 'mapr:maprmapr' "https://dsr-demo-pbmggt.se.corp.maprtech.com:8443/rest/table/index/add?path=/user/mapr/demo.mapr.com/tables/payments&index=idx_payer&indexedfields=payer:1"
##{"timestamp":1525388489727,"timeofday":"2018-05-03 11:01:29.727 GMT+0000 PM","status":"ERROR","errors":[{"id":22,"desc":"Failed to add index for table: /user/mapr/demo.mapr.com/tables/payments : Cluster Gateways are not configured"}]}mapr@edge-5d5bbc5544-sbgs4:~$

#In MapR-DB Shell, try queries on payments payers and compare with previous query performance:
$ /opt/mapr/bin/mapr dbshell
maprdb mapr:> find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
maprdb mapr:> ctrl-c


#In Drill try 
$ sqlline
select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer='CorMatrix Cardiovascular Inc.';
select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer like '%Dental%';
select  distinct(payer) from dfs.`/user/mapr/demo.mapr.com/tables/payments` ;

##Cleaning Up

#You can delete the topic and table using the following command from a container terminal:
#```
#maprcli stream topic delete -path /mapr/maprdemo.mapr.io/apps/paystream -topic payment
#maprcli table delete -path /mapr/maprdemo.mapr.io/apps/payments

#```
## Conclusion
#
#In this example you have learned how to:

#* Publish using the Kafka API  Medicare Open payments data from a CSV file into MapR-ES 
#* Consume and transform the streaming data with Spark Streaming and the Kafka API
#* Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector
#* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL 
#* Query the MapR-DB document database using Apache Drill 
#* Query the MapR-DB document database using Java and the OJAI library



You can also look at the following examples:

* [mapr-db-60-getting-started](https://github.com/mapr-demos/mapr-db-60-getting-started) to learn Discover how to use DB Shell, Drill and OJAI to query and update documents, but also how to use indexes.
* [Ojai 2.0 Examples](https://github.com/mapr-demos/ojai-2-examples) to learn more about OJAI 2.0 features
* [MapR-DB Change Data Capture](https://github.com/mapr-demos/mapr-db-cdc-sample) to capture database events such as insert, update, delete and react to this events.