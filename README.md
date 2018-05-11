# Heathcare Data - Streaming ETL Pipeline and Data Exploration on ACA Open Payments Dataset
## (Using Spark, JSON, MapR-DB, MapR-ES, Drill, and Tableau)
This Demonstration was adapted and augmented, from Carol McDonald's original version (see References section below), for deployment and use in the MapR SE CLuster, 
In addition to porting the code to work on the SE Cluster, Tableau Visualizations, Drill views, and ingesting additional data fields, were added to Carol's original content.

## Introduction

This example will demonstrate working with MapR-ES, Spark Streaming, MapR-DB JSON, Drill, and Tableau on MapR-DB.

- Publish using the Kafka API Medicare Open payments data from a CSV file into MapR-ES 
- Consume and transform the streaming data with Spark Streaming and the Kafka API, and Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
- Load data into Spark Dataset: Query the MapR-DB JSON document database with Spark-SQL, using the Spark-DB connector
- Query the MapR-DB document database using Apache Drill. 
- Query the MapR-DB document database using Java and the OJAI library
- Tableau Reports: connect Tableau desktop and run a report that is regularly updated with new data that is streaming into MapR.

## Demo: Step-by-Step
Important - This readme is a basic explanation and how-to for the technical components of the demo. 
See the SE wiki for the full Demo narrative, with architecture diagrams (http://wiki.se.corp.maprtech.com) - do not skip that component.

#### 0  Preparing the environment
deployment steps in Applariat

Create Drill views on the MapR-DB payments table, for use with queries and Tableau Desktop reports that connect to MapR-DB using Drill
        
In a new terminal window, ssh to the cluster edge node as 'mapr' and:
 
        sqlline
        !run /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/createDrillViews.sql
        !quit


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


#### 3  Create Drill views on the MapR-DB payments table, for use with queries and Tableau Desktop reports that connect to MapR-DB using Drill
Create the Drill views to use in Tableau reports.  Tableau-Drill requires views, and does not access the MapR-DB table directly.

Run these from a terminal window (connected by ssh to the cluster edge node as 'mapr')

To Start Drill shell:

        sqlline

Change your working schema to create Drill views in the dfs.tmp schema:

        use dfs.tmp;

        create or replace view physicians_by_revenue as 
            select physician_id, sum(amount) as revenue 
            from dfs.`/user/mapr/demo.mapr.com/tables/payments` 
            group by physician_id;

        create or replace view physicians_by_specialty_revenue as 
            select physician_specialty,sum(amount) as total 
            from dfs.`/user/mapr/demo.mapr.com/tables/payments` 
            group by physician_specialty;

        create or replace view aca_open_payments as
            select recipient_country, recipient_state, physician_specialty, recipient_zip, payer, nature_of_payment, (sum(amount)) as us_dollars
            from dfs.`/user/mapr/demo.mapr.com/tables/payments`
            GROUP BY recipient_country, recipient_state, recipient_zip, physician_specialty, payer, nature_of_payment;


#### 4  Query the payments table in MapR-DB JSON, with Spark SQL
This spark job loads data from MapR-DB JSON (using the MapR-DB Spark connector), into a Spark Dataset (an in-memory RDD optimized for performance), then runs Spark-SQL to query that data

In a new terminal window, ssh to the cluster edge node as 'mapr' and:

        $SPARK_PATH/bin/spark-submit --class sparkmaprdb.QueryPayment --master local[2] /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


#### 5  Query the MapR-DB document database using Apache Drill (via JDBC)
Apache Drill is an open source, low-latency query engine for big data that delivers interactive SQL analytics at petabyte scale. Drill provides a massively parallel processing execution engine, built to perform distributed query processing across the various nodes in a cluster.

Run these queries from a terminal window (connected by ssh to the cluster edge node as 'mapr') and type 'sqlline' to enter the drill shell.

To Start Drill shell:

        sqlline

Show me the physician specialties having the highest total of recorded payments - top 10:

        select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty order by total desc limit 10;
        
Show me the physicians recieving the greatest payments - top 5:

        select physician_id, sum(amount) as revenue from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_id order by revenue desc limit 5;

Show me the payment catagories with the highest total payment amounts - top 5:

        select nature_of_payment,  sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by nature_of_payment order by total desc limit 5;

Look up a physican's name and total payments recieved, from their 'id': 

        select ,  amount from dfs.`/user/mapr/demo.mapr.com/tables/payments` where _id like '98485%';

Show me all the payments made by a specific payor:

        select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer='CorMatrix Cardiovascular Inc.';
        select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer like '%Dental%';

Show me all the payors, that made payments, in our sample dataset:

        select  distinct(payer) from dfs.`/user/mapr/demo.mapr.com/tables/payments`;

To exit Drill shell:

        !quit


#### 6  Query the MapR-DB document database using Java and the OJAI library
OJAI, is the opes source Java API used to access MapR-DB JSON.  It leverages the same query engine as MapR-DB Shell and Apache Drill to query the payments table.

To Query the MapR-DB payments table using OJAI:

        $SPARK_PATH/bin/spark-submit --class maprdb.OJAI_SimpleQuery --master local[2] --jars /opt/mapr/drill/jars/jdbc-driver/drill-jdbc-all-1.11.0.jar /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar


#### 7  Query the MapR-DB payments table using the MapR-DB shell, dbshell
Run these queries from a terminal window (connected by ssh to the cluster edge node as 'mapr')

To start MapR-DB shell:

        /opt/mapr/bin/mapr dbshell
        
Set the environment for your session (To learn more about the various commands, run help or help <command> , for example help insert):

        jsonoptions --pretty true --withtags false

Show me 5 documents

        find /user/mapr/demo.mapr.com/tables/payments --limit 5

Queries by '_id' will be faster because '_id' is the primary index on the payment table
Show me Documents with a physician id that starts with 98485:

        find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"98485%"} }' --f _id,amount

Show me documents for Payments made in February:

        find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"%_02/%"} }' --f _id,amount

Show me all payors:

        find /user/mapr/demo.mapr.com/tables/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment

To exit MapR-DB shell:

        ctrl-C


#### 8  Adding a secondary index to the payments JSON table, to improve query performance
Run these queries from a terminal window (connected by ssh to the cluster edge node as 'mapr')

Run db-shell queries, without a secondary index on the payments table, and note query performance:

        /opt/mapr/bin/mapr dbshell
        find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
        ctrl-c

Create a secondary index on the MapR-DB JSON payments table:

        maprcli create index (maprcli table index add -path /apps/payments -index idx_payer -indexedfields 'payer:1')
        or
        curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/table/index/add?path=/user/mapr/demo.mapr.com/tables/payments&index=idx_payer&indexedfields=payer,"
        or 
        Build index on table in MCS

Again,run db-shell queries on payments table, and compare with query performance observed before adding secondary index:

        /opt/mapr/bin/mapr dbshell
        find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
        ctrl-c


#### 9  Connect Tableau and open a saved report
This step assumes you have the Tableau desktop installed on your laptop and explains how to connect the desktop client to Drill, running on your SE Cluster deployment.  Tableau trial license keys for SE's are available from the FE team (see References section)

To connect tableau desktop to the Drill service on your SE Cluster deployment:

        Open Tableau Desktop
        

#### 10  Other References:

[Carol's Blog](https://mapr.com/blog/streaming-data-pipeline-transform-store-explore-healthcare-dataset-mapr-db/) from which this Demo originated.  Thanks Carol!

[SE private Git Repository for this Demo](http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git) , the master repo for this SE Cluster version of the Demo.

[Tableau Desktop Licenses for SE's](https://docs.google.com/spreadsheets/d/1A1OGD0mY-eLSBM7hgU7eRx45PXt4oohRVv6493IdJJM/edit#gid=193582544) , as tracked by the FE team.

[mapr-db-60-getting-started](https://github.com/mapr-demos/mapr-db-60-getting-started) to learn Discover how to use DB Shell, Drill and OJAI to query and update documents, but also how to use indexes.

[Ojai 2.0 Examples](https://github.com/mapr-demos/ojai-2-examples) to learn more about OJAI 2.0 features.

[MapR-DB Change Data Capture](https://github.com/mapr-demos/mapr-db-cdc-sample) to capture database events such as insert, update, delete and react to this events.

#### End