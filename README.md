# Heathcare Data - Streaming ETL Pipeline and Data Exploration 
## (Using Spark, JSON, MapR-DB, MapR-ES, Drill, and Tableau)
This Demonstration was adapted for deployment on the MapR SE CLuster, from Carol McDonald's blog @ https://mapr.com/blog/streaming-data-pipeline-transform-store-explore-healthcare-dataset-mapr-db/
Tableau Visualizations, Drill views, and ingesting additional data fields, were added to the original content.

## Introduction

This example will demonstrate working with MapR-ES, Spark Streaming, MapR-DB JSON, Drill, and Tableau on MapR-DB.

- Publish using the Kafka API Medicare Open payments data from a CSV file into MapR-ES 
- Consume and transform the streaming data with Spark Streaming and the Kafka API, and Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
- Load data into Spark Dataset: Query the MapR-DB JSON document database with Spark-SQL, using the Spark-DB connector
- Query the MapR-DB document database using Apache Drill. 
- Query the MapR-DB document database using Java and the OJAI library
- connect Tableau desktop and run a report that is regularly updated with new data that is streaming into MapR.

## Demo: Step-by-Step

#### 1  Publish the 'ACA Medicare Open Payments' dataset into MapR-ES (using the MapR Kafka API)
This simple producer client application reads lines from the payments.csv file and publishes them in their original comma-delimited format, to the MapR Stream:topic @ /streams/paystream:payments.

The paystream:payments stream:topic can be viewed in MCS @ path /mapr/${MAPR_CLUSTER/user/mapr/demo.mapr.com/streams/paystream
        
To launch the producer: In a new terminal window, ssh to the cluster edge node as 'mapr' and:

        cd /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments
        java -cp /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer


#### 2  Read the MapR-ES topic and transform the data with Spark Streaming (using the MapR-ES Kafka API), and write to MapR-DB (using the Spark MapR-DB connector)
This Spark-Streaming consumer client application accomplishes three tasks:  First, it reads each incoming message from the MapR stream:topic @ /streams/paystream:payments using the MapR Kafka API. Then, the data is loaded into Spark RDD's (in mempory) and transformed with Spark Streaming, to JSON format. And lastly, each record (JSON array) is written to the 'payments' table in the MapR-DB document database.
 
The MapR-DB JSON 'payments' table can be viewed in MCS @ path /user/mapr/demo.mapr.com/tables/payments

To launch the consumer: In a new terminal window, ssh to the cluster edge node as 'mapr' and:

        $SPARK_PATH/bin/spark-submit --class streaming.SparkKafkaConsumer --master local[2] /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


#### 3  Query the payments table in MapR-DB JSON, with Spark SQL
This spark job loads data from MapR-DB JSON (using the MapR-DB Spark connector), into a Spark Dataset (an in-memory RDD optimized for performance), then runs Spark-SQL to query that data

In a new terminal window, ssh to the cluster edge node as 'mapr' and:

        $SPARK_PATH/bin/spark-submit --class sparkmaprdb.QueryPayment --master local[2] /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


#### 4  Query the MapR-DB document database using Apache Drill (via JDBC)
Apache Drill is an open source, low-latency query engine for big data that delivers interactive SQL analytics at petabyte scale. Drill provides a massively parallel processing execution engine, built to perform distributed query processing across the various nodes in a cluster.

Show me the 10 physician specialties having the highest total of recorded payments:

        sqlline
        select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty order by total desc limit 10;
        quit!


#### 5 - Query the MapR-DB document database using Java and the OJAI library
OJAI, is the opes source Java API used to access MapR-DB JSON.  It leverages the same query engine as MapR-DB Shell and Apache Drill to query the payments table.

To Query the MapR-DB payments table using OJAI:

        $SPARK_PATH/bin/spark-submit --class maprdb.OJAI_SimpleQuery --master local[2] --jars /opt/mapr/drill/jars/jdbc-driver/drill-jdbc-all-1.11.0.jar /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


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