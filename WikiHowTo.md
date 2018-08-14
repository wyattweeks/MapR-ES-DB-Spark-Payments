======Healthcare Data - Streaming ETL Pipeline and Data Exploration on ACA Open Payments Dataset======
====Using Spark, JSON, MapR-DB, MapR-ES, Drill, and Tableau====
This Demonstration was adapted and augmented, from Carol McDonald's original version (see References section below), for deployment and use in the MapR SE CLuster, 
In addition to porting the code to the SE Cluster, Tableau Visualizations, Drill views, and the ingestion of additional data fields, were added to Carol's original content.

Developer git repo: http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git

====Introduction====

**This demo showcases MapR as a full-solution, streaming data platform:**

  - Medicare Open payments data will be ingested from a CSV file into a MapR-ES stream, using the MapR Kafka API. 
  - Data from this stream will be ingested and transformed to JSON using Spark Streaming.
  - The JSON data will be written to the MapR-DB document database using the Spark-DB connector.
  - Data will be loaded into a Spark Dataset by querying the MapR-DB JSON table with Spark-SQL, using the Spark-DB connector.
  - You will query the MapR-DB document database using Apache Drill, as well as Java and OJAI.
  - You will run a Tableau Desktop report to illustrate the near-real-time analytic capabilities of this overall solution.

=====Step-by-Step Demo Instructions=====
**Important:** What follows is a basic explanation and how-to for the technical components of the demo. The demo is complex.  You will need to practice in advance of presenting this to a customer for the first time!
See the SE wiki for the full Demo narrative (doc under construction..use Carol's original blog see link in references section below, or compile your own!)

-------------------------------------------------------
====Prepare the environment in advance of your demo====
-------------------------------------------------------
**Pre-requisites** 
    - If you don't already have Tableau desktop installed on your laptop, install it now. Tableau trial license keys for SE's are available from the FE team:
        - Trial license keys https://docs.google.com/spreadsheets/d/1A1OGD0mY-eLSBM7hgU7eRx45PXt4oohRVv6493IdJJM/edit#gid=1906923175
        - Desktop Trial Software https://www.tableau.com/products/trial
        - Desktop Version 10.3 has been tested with this demo
    - You must be connected to the MapR Corporate VPN

**Deploy and Verify the Demo Cluster**
    - Deploy the demo cluster in AppLariat:
      - https://apl.se.corp.maprtech.com
      - In the Appleft navbar, click on '**Deploy**'
      - Go to the '**Healthcare**' Applications section and find the '**ACA Open Payments Data: MapR-ES-DB-Spark-Tableau on MapR**' release and click deploy
      - Define the length of the lease you will need (note: default of 'short term', will stop the cluster every 30min)
      - You will be directed to the deployment dashboard status page
      - wait for all of the components to come up (green) and status '**Deployed**'
    - Login to MCS and check the status of the cluster
      - on the AppLariat deployment deployment page, go to the 'Application Summary' section, and click on the hyperlink to MCS
      - you should see volumes named 'files', 'tables', and 'streams', and a table named 'payments'
    - Create Drill views on the MapR-DB payments table.  Connecting Tableau to Drill, requires views, as Tableau does not access MapR-DB tables directly. 
      - On the App Lariat deployment page, find the DNS for your edge node
        * locate the edge 'component'
        * copy the DNS
      - in a new terminal window, ssh to your edge node and run the script to create Drill views:<code bash> ssh mapr@theDNSyoucopied</code> <code> password = maprmapr</code> <code bash>sqlline</code> <code bash>!run /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/createDrillViews.sql</code> <code bash>!quit</code>
    - Copy the pre-built Tableau report, to your desktop
      - From the terminal window:<code bash>scp mapr@edge-XYZ123.se.corp.maprtech.com:/public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/tableau/Healthcare_Payments_Map_Report.twb ~/Desktop/Healthcare_Payments_Map_Report.twb</code>
    - **Finally, verify the demo deployment is functioning correctly and ready to demo**  ,by connecting the Tableau desktop report, to the Drill service on your SE Cluster deployment:
      - Open your Tableau Desktop Application
      - Open the 'Healthcare_Payments_Map_Report.twb' report you downloaded in step 4
      - When the report opens, you will be prompted for a password to connect to the cluster:
        * click on 'Edit Connection' in this window, to edit the data source and connect to your cluster:
          * connect: Direct 
          * **Note**  Tableau can also connect to Zookeeper, for load-balancing and failover (this is recommended for Production systems)
          * server: External IP address obtained from app lariat 'mdn' container of your cluster deployment
          * port: 3110
          * authentication: Username and Password
          * username: mapr
          * password: maprmapr
       - Once connected, refresh the connection, and save the report
       - select the 'Payor' sheet in the Tableau workbook, and enter 'presentation view'
       - **You are now ready to demonstrate the report!**

----------------------------------
====Customer Facing Demo Steps====
----------------------------------
===1. Publish the 'ACA Medicare Open Payments' .csv dataset into a MapR-ES stream (using the MapR Kafka API)===
Demonstrate this simple producer client application reading 30,000 lines from the payments.csv file in seconds.
Messages are published in their original comma-delimited format, to the MapR Stream:topic  'paystream:payments'
  * The stream:topic can be viewed in MCS at the path:
    * /mapr/${MAPR_CLUSTER/user/mapr/demo.mapr.com/streams/paystream
  * Launch the producer application
    * In a new terminal window, ssh to the cluster edge node as 'mapr':<code bash>
cd /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments</code> <code java>java -cp /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer
</code>
===2. Read the MapR-ES topic and transform the data with Spark Streaming (using the MapR-ES Kafka API), and write to MapR-DB (using the Spark MapR-DB connector)===
Show this Spark-Streaming consumer client application reading 30,000 messages from the MapR stream, transforming them to JSON, and writing them to MapR-DB, in seconds.  

  * This consumer client application has three functions:
    - First, it reads each incoming message from the MapR stream:topic @ /streams/paystream:payments using the MapR Kafka API 
    - Then, the data is loaded into Spark RDD's (in memory) and transformed with Spark Streaming, to JSON format 
    - And lastly, each record (JSON array) is written to the 'payments' table in the MapR-DB document database.
  * The MapR-DB JSON 'payments' table can be viewed in MCS at path:
    * /user/mapr/demo.mapr.com/tables/payments
  * Launch the consumer application
    * In a new terminal window, ssh to the cluster edge node as 'mapr':<code java>
$SPARK_PATH/bin/spark-submit --class streaming.SparkKafkaConsumer --master local[2] /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar
</code>

===3. Connect Tableau Desktop to the MapR-DB JSON table and run a report==
This Tableau report is updated automatically, so that new data streaming into the MapR-DB 'payments' table is dynamically displayed on the Tableau report, in near-real-time.

You can start by simply opening and demonstrating the 'Healthcare_Payments_Map_Report.twb' Tableau report you prepared while deploying and verifying the cluster.  

Or (optionally) you can first demonstrate the simplicity of connecting Tableau to MapR, and then walk-thru the report.
  * To illustrate connecting Tableau desktop to Drill on the cluster:
    - Open Tableau Desktop Application
    - Open the 'Healthcare_Payments_Map_Report.twb' report you downloaded
    - When the report opens, you will be prompted for a password to connect to the cluster - click on 'Edit Connection' in this window, to edit the data source and connect to your cluster:
      - connect: Direct
      - server: External IP address obtained from app lariat 'mdn' container of your cluster deployment
      - port: 3110
      - authentication: Username and Password
      - username: mapr
      - password: maprmapr
    - Once connected, refresh the connection, select the 'Payor' sheet in the Tableau workbook, and enter 'presentation view' to demonstrate the report


=== The remaining steps below, are demonstrated in the command line interface, and therefore will not be applicable to all audiences===       
=== 4. Query the payments table in MapR-DB JSON, with Spark SQL===
This spark job loads data from MapR-DB JSON (using the MapR-DB Spark connector), into a Spark Dataset (an in-memory RDD optimized for performance), then runs Spark-SQL to query that data

  * In a new terminal window, ssh to the cluster edge node as 'mapr':<code>$SPARK_PATH/bin/spark-submit --class sparkmaprdb.QueryPayment --master local[2] /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar</code>


=== 5. Query the MapR-DB document database using Apache Drill (via JDBC)===
Apache Drill is an open source, low-latency query engine for big data that delivers interactive SQL analytics at petabyte scale. Drill provides a massively parallel processing execution engine, built to perform distributed query processing across the various nodes in a cluster.

Run these commands from a terminal window (ssh to the cluster edge node as 'mapr')

  * To start the drill shell:<code>sqlline</code>

  * Show me the physician specialties having the highest total of recorded payments - top 10:<code sql>select physician_specialty,sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_specialty order by total desc limit 10;</code>
        
  * Show me the physicians recieving the greatest payments - top 5:<code>select physician_id, sum(amount) as revenue from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by physician_id order by revenue desc limit 5;</code>

  * Show me the payment catagories with the highest total payment amounts - top 5:<code>select nature_of_payment, sum(amount) as total from dfs.`/user/mapr/demo.mapr.com/tables/payments` group by nature_of_payment order by total desc limit 5;</code>

  * Look up a physican's name and total payments recieved, from their 'id':<code>select amount from dfs.`/user/mapr/demo.mapr.com/tables/payments` where _id like '98485%';</code>
  
  * Show me all the payments made by a specific payor:<code>select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer='CorMatrix Cardiovascular Inc.';</code> <code>select _id, amount, payer from dfs.`/user/mapr/demo.mapr.com/tables/payments` where payer like '%Dental%';</code>

  * Show me all the payors, that made payments, in our sample dataset:<code>select  distinct(payer) from dfs.`/user/mapr/demo.mapr.com/tables/payments`;</code>
  
  * To exit Drill shell:<code>!quit</code>

=== 6. Query the MapR-DB document database using Java and the OJAI library===
OJAI, is the opes source Java API used to access MapR-DB JSON.  It leverages the same query engine as MapR-DB Shell and Apache Drill to query the payments table.

  * To Query the MapR-DB payments table using OJAI:<code>$SPARK_PATH/bin/spark-submit --class maprdb.OJAI_SimpleQuery --master local[2] --jars /opt/mapr/drill/jars/jdbc-driver/drill-jdbc-all-1.11.0.jar /public_data/demos_healthcare/MapR-ES-DB-Spark-Payments/target/mapr-es-db-spark-payment-1.0.jar</code>


=== 7. Query the MapR-DB payments table using the MapR-DB shell, dbshell ===
Run these commands from a terminal window (connected by ssh to the cluster edge node as 'mapr')

  * To start MapR-DB shell:<code>/opt/mapr/bin/mapr dbshell</code>
        
  * Set the environment for your session (To learn more about the various commands, run help):<code>jsonoptions --pretty true --withtags false</code>

  * Show me 5 documents<code>find /user/mapr/demo.mapr.com/tables/payments --limit 5</code>

** Queries by '_id' will be faster because '_id' is the __primary index__ on the payment table**
  * Show me Documents with a physician id that starts with 98485:<code>find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"98485%"} }' --f _id,amount</code>

  * Show me documents for Payments made in February:<code>find /user/mapr/demo.mapr.com/tables/payments --where '{ "$like" : {"_id":"%_02/%"} }' --f _id,amount</code>

  * Show me all payors:<code>find /user/mapr/demo.mapr.com/tables/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment</code>

  * To exit MapR-DB shell:<code>ctrl-C</code>

===8. Adding a Secondary Index to the payments JSON table, to improve query performance===
Run these commands from a terminal window (connected by ssh to the cluster edge node as 'mapr')

  * Run db-shell queries, **without** a __secondary index__ on the payments table, and note query performance:<code>/opt/mapr/bin/mapr dbshell</code> <code>find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment</code> <code>ctrl-c</code>

  * Create a __secondary index__ on the MapR-DB JSON payments table, using **one** of the following three methods:
    * Using the maprcli:<code>maprcli create index (maprcli table index add -path /apps/payments -index idx_payer -indexedfields 'payer:1')</code>
    * or, using the REST command:<code>curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/table/index/add?path=/user/mapr/demo.mapr.com/tables/payments&index=idx_payer&indexedfields=payer,"</code>
    * or, using MCS:<code>view the /apps/payments table properties in MCS
index name = idx_payer
indexedfields = payer:1</code>

  * Now that the index has been built, re-run the db-shell queries on payments table, and note the performance improvement over when the queries were run with no secondary index on the table:<code>/opt/mapr/bin/mapr dbshell</code> <code>find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment</code> <code>ctrl-c</code>
        
===References and Other Information:===

  * to get stream info, from the cli using REST:<code>curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/stream/info?path=/user/mapr/demo.mapr.com/streams/paystream" | python -m json.tool</code>

  * to get the topic info for the stream, from the cli using REST:<code>curl -sSk -X POST -u ${MAPR_ADMIN}:${MAPR_ADMIN_PASSWORD} "${MCS_URL}/rest/stream/topic/info?path=/user/mapr/demo.mapr.com/streams/paystream&topic=payments" | python -m json.tool</code>

  * Carol McDonald's Original Blog, upon which this demo is based.  Thanks Carol!  https://mapr.com/blog/streaming-data-pipeline-transform-store-explore-healthcare-dataset-mapr-db/

  * SE private Git Repository for this Demo  http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git

  * Tableau Desktop Licenses for SE's.  Thanks FE Team!  https://docs.google.com/spreadsheets/d/1A1OGD0mY-eLSBM7hgU7eRx45PXt4oohRVv6493IdJJM/edit#gid=1906923175

  * mapr-db-60-getting-started: Hands on exercise to learn how to use DB Shell, Drill and OJAI to query and update documents.  Also, learn how to use MapR-DB table indexes.  https://github.com/mapr-demos/mapr-db-60-getting-started

  * Ojai 2.0 Examples, to learn more about OJAI 2.0 features  https://github.com/mapr-demos/ojai-2-examples

  * MapR-DB Change Data Capture: Hands on example showing how to capture database events in CDC, such as insert/update/delete, and event-based actions  https://github.com/mapr-demos/mapr-db-cdc-sample

===End===