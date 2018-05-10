## Streaming ETL Pipeline to Transform, Store and Explore Healthcare Dataset using Spark, JSON, MapR-DB, MapR-ES, Drill, and Tableau

# Git Repo for Project
MapR-ES-DB-Spark-Payments project has been cloned to /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments (source = git clone http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git)
Manually refresh when repo changes (see steps below)
maven rebuilds jars in /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target

    How to manually refresh the project on /public_data, from se git repo:
        1 - ssh to jump box as mapr
        2 - $ cd /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments
        3 - $ git pull 
        4 - $ mvn clean install

# Introduction

This example will show you how to work with MapR-ES, Spark Streaming, and MapR-DB JSON :
1 - Publish using the Kafka API Medicare Open payments data from a CSV file into MapR-ES 
2 - Consume and transform the streaming data with Spark Streaming and the Kafka API, AND Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
3 - Load data into Spark Dataset: Query the MapR-DB JSON document database with Spark-SQL, using the Spark-DB connector
4 - Query the MapR-DB document database using Apache Drill. 
5 - Query the MapR-DB document database using Java and the OJAI library
6 - connect Tableau desktop and run a report that is regularly updated with new data that is streaming into MapR.


# DEMO: STEP-BY-STEP

0 - Launching the demo cluster
   - open the drill ports

1 - Publish using the Kafka API Medicare Open payments data from a CSV file into MapR-ES 
This java publisher client will read lines from the payments.csv and publish them in the same format (comma-delimited strings) to the MapR Stream:topic @ /streams/paystream:payments
The paystream:payments stream:topic can be viewed in MCS @ path /mapr/${MAPR_CLUSTER}/user/mapr/demo.mapr.com/streams/paystream
Open a separate terminal window and ssh to the cluster edge node as mapr and:
cd /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments
java -cp /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer


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
# For presentation purposes, you may more than one consumer client manually, from a separate terminal window
# Note: you'll see the client looking for data in the stream, but unless you stop the auto-deployed consumer, it won't actually consume data from the stream,
#      as the original consumer client is capable of consuming all the data being produced.  OR the original consumer client may fail (due to a conflict in reading 
#      from the stream partition), in which case, this newly launched consumer will pick up where the other left off) See future enhancement on this.
#
# in a new terminal window, ssh to the cluster edge node as mapr and:
#
$SPARK_PATH/bin/spark-submit --class streaming.SparkKafkaConsumer --master local[2] /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


### 3 - Load data into Spark Dataset AND Query the MapR-DB JSON document database using Spark-SQL, using the MapR-DB Spark connector
#
# in a new terminal window, ssh to the cluster edge node as mapr and:
$SPARK_PATH/bin/spark-submit --class sparkmaprdb.QueryPayment --master local[2] ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


### 4 - Query the MapR-DB document database using Apache Drill (JDBC). 
# (old) java version: java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* maprdb.DRILL_SimpleQuery
# (old) Spark version: cd $SPARK_PATH/bin
#                      ./spark-submit --class maprdb.DRILL_SimpleQuery --master local[2] --jars /opt/mapr/drill/jars/jdbc-driver/drill-jdbc-all-1.11.0.jar ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar
#
# Show the Top 10 physician specialties by total payments
sqlline
select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty order by total desc limit 10;
quit!


### 5 - Query the MapR-DB document database using Java and the OJAI library - OPTIONAL
# OJAI, the Java API used to access MapR-DB JSON, leverages the same query engine as MapR-DB Shell and Apache Drill to query payments table
# Query the MapR-DB payments table using OJAI
# 
# cd $SPARK_PATH/bin
#./spark-submit --class maprdb.OJAI_SimpleQuery --master local[2] --jars /opt/mapr/drill/jars/jdbc-driver/drill-jdbc-all-1.11.0.jar ~/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


#### 6. Using the MapR-DB shell and Drill from your mac client - JSON documents
#Refer to [**connecting clients **](https://maprdocs.mapr.com/home/MapRContainerDevelopers/ConnectingClients.html) for 
#information on setting up the Drill client
#**Use MapR DB Shell to Query the Payments table**
#In this section you will  use the DB shell to query the Payments JSON table
#To access MapR-DB from your mac client or logged into the container, you can use MapR-DB shell:
$ /opt/mapr/bin/mapr dbshell
#from the mapr-db shell: (To learn more about the various commands, run help or help <command> , for example help insert.)
$ jsonoptions --pretty true --withtags false
#**find 5 documents**
find /user/mapr/demo.mapr.com/tables/payments --limit 5
###Note that queries by _id will be faster because _id is the primary index
#Query document with Condition _id starts with 98485 (physician id)**
find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"98485%"} }' --f _id,amount
#**Query document with Condition _id has february**
find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"%_02/%"} }' --f _id,amount
#**find all payers=**
find /user/mapr/demo.mapr.com/tables/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
# ctrl-C to exit db-shell


#  Use Drill Shell to query MapR-DB - Rows in a table
# From an edge node terminal, connect to Drill as user mapr through JDBC by running sqlline:
#               (this is CM's original cmd: /opt/mapr/drill/drill-1.11.0/bin/sqlline -u "jdbc:drill:drillbit=localhost" -n mapr)
# On edge node, open Drill shell: 
sqlline
# 0: jdbc:drill:zk=mapr-zk:5181/drill/${MAPR_CLUSTER}->
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
!quit

#### 7. Adding a secondary index to the payments JSON table, to speed up queries
#
# check BEFORE performance:
/opt/mapr/bin/mapr dbshell
find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
ctrl-c
# In a terminal window: maprcli create index (maprcli table index add -path /apps/payments -index idx_payer -indexedfields 'payer:1')
# need to convert this to REST
# format:  http[s]://<host>:<port>/rest/table/index/add?path=<path>&index=<index name>&indexedfields=<indexed field names>
# curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/table/index/add?path=/user/mapr/demo.mapr.com/tables/payments&index=idx_payer&indexedfields=payer,"
# ERROR: {"timestamp":1525735089672,"timeofday":"2018-05-07 11:18:09.672 GMT+0000 PM","status":"ERROR","errors":[{"id":22,"desc":"Failed to add index for table: /user/mapr/demo.mapr.com/tables/payments : Cluster Gateways are not configured"}]}#
#
# REST CMD NOT WORKING - BUILD IN MCS
#
# again,run queries on payments/payers and compare with previous query performance:
# In db-shell
/opt/mapr/bin/mapr dbshell
find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
ctrl-c
# In Drill shell 
$ sqlline
select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer='CorMatrix Cardiovascular Inc.';
select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer like '%Dental%';
select  distinct(payer) from dfs.`/user/mapr/demo.mapr.com/tables/payments` ;
!quit

### 8. Create a Drill view and Query it with Tableau Desktop
# avoid using 'linit' in drill view create statements - can result in IOOB errors
use dfs.tmp;
#
create or replace view physicians_by_revenue as select physician_id, sum(amount) as revenue from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_id;
#
create or replace view physicians_by_specialty_revenue as select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty;
# 
create or replace view aca_open_payments as
select recipient_country, recipient_state, physician_specialty, recipient_zip, payer, nature_of_payment, (sum(amount)) as us_dollars
from dfs.`/user/mapr/demo.mapr.com/tables/payments`
GROUP BY recipient_country, recipient_state, recipient_zip, physician_specialty, payer, nature_of_payment;
# AIzaSyCCVEaGnTYrPh394VS9V2X6gSZRSKkOBy0
# https://www.google.com/maps/embed/v1/view?key=AIzaSyCCVEaGnTYrPh394VS9V2X6gSZRSKkOBy0&center=39.5501,105.7821&zoom=8&maptype=roadmap


### Cleaning Up
# You can delete the topic and table in MCS

### Conclusion
#
# In this example you have learned how to:
#
# * Publish using the Kafka API  Medicare Open payments data from a CSV file into MapR-ES 
# * Consume and transform the streaming data with Spark Streaming and the Kafka API
# * Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector
# * Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL 
# * Query the MapR-DB document database using Apache Drill 
# * Query the MapR-DB document database using Java and the OJAI library
# Other References:
#
# * [mapr-db-60-getting-started](https://github.com/mapr-demos/mapr-db-60-getting-started) to learn Discover how to use DB Shell, Drill and OJAI to query and update documents, but also how to use indexes.
# * [Ojai 2.0 Examples](https://github.com/mapr-demos/ojai-2-examples) to learn more about OJAI 2.0 features
# * [MapR-DB Change Data Capture](https://github.com/mapr-demos/mapr-db-cdc-sample) to capture database events such as insert, update, delete and react to this events.