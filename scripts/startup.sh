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

#### 0. Use REST to create volumes
# create volumes for files tables and streams
#test on sandbox
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

# need to install git and clone project (may need to do install 2x?)
sudo apt-get install git
y
git clone http://git.se.corp.maprtech.com/wweeks/MapR-ES-DB-Spark-Payments.git

sudo apt-get install maven
y

#Create the following jars.
#`mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0.jar`
# `mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0-jar-with-dependencies.jar`

cd ~/MapR-ES-DB-Spark-Payments
mvn clean install


##Run the java publisher and the Spark consumer**

#This client will read lines from the file in ./data/payments.csv and publish them to the topic /apps/paystream:payments. 
#You can optionally pass the file and topic as input parameters <file topic> 
java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer

###This spark streaming client will consume from the topic /apps/paystream:payments and write to the table /apps/payments.
You can wait for the java client to finish, or from a separate mac terminal you can run the spark streaming consumer with the following command, 
#or you can run from your IDE :
#You can optionally pass the topic and table as input parameters <topic table> 
cd ~/MapR-ES-DB-Spark-Payments
java -cp ./target/mapr-es-db-spark-payment-1.0-jar-with-dependencies.jar:./target/* streaming.SparkKafkaConsumer



mapr@edge-5d5bbc5544-sbgs4:~/MapR-ES-DB-Spark-Payments$ java -cp ./target/mapr-es-db-spark-payment-1.0-jar-with-dependencies.jar:./target/* streaming.SparkKafkaConsumer
Using hard coded parameters unless you specify the consume topic and table. <topic table>
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/05/02 21:09:44 INFO SparkContext: Running Spark version 2.1.0-mapr-1710
18/05/02 21:09:45 INFO log: Logging initialized @2097ms
18/05/02 21:09:45 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/05/02 21:09:46 INFO SecurityManager: Changing view acls to: mapr
18/05/02 21:09:46 INFO SecurityManager: Changing modify acls to: mapr
18/05/02 21:09:46 INFO SecurityManager: Changing view acls groups to:
18/05/02 21:09:46 INFO SecurityManager: Changing modify acls groups to:
18/05/02 21:09:46 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(mapr); groups with view permissions: Set(); users  with modify permissions: Set(mapr); groups with modify permissions: Set()
18/05/02 21:09:46 INFO Utils: Successfully started service 'sparkDriver' on port 40540.
18/05/02 21:09:46 INFO SparkEnv: Registering MapOutputTracker
18/05/02 21:09:46 INFO SparkEnv: Registering BlockManagerMaster
18/05/02 21:09:46 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/05/02 21:09:46 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/05/02 21:09:46 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-027fa3a8-5154-41e3-9748-e16a4c092501
18/05/02 21:09:46 INFO MemoryStore: MemoryStore started with capacity 12.4 GB
18/05/02 21:09:46 INFO SparkEnv: Registering OutputCommitCoordinator
18/05/02 21:09:46 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/05/02 21:09:46 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://10.233.90.225:4040
18/05/02 21:09:46 INFO Executor: Starting executor ID driver on host localhost
18/05/02 21:09:46 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 34977.
18/05/02 21:09:46 INFO NettyBlockTransferService: Server created on 10.233.90.225:34977
18/05/02 21:09:46 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/05/02 21:09:46 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 10.233.90.225, 34977, None)
18/05/02 21:09:46 INFO BlockManagerMasterEndpoint: Registering block manager 10.233.90.225:34977 with 12.4 GB RAM, BlockManagerId(driver, 10.233.90.225, 34977, None)
18/05/02 21:09:46 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 10.233.90.225, 34977, None)
18/05/02 21:09:46 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 10.233.90.225, 34977, None)
-------------------------------------------
Time: 1525295388000 ms
-------------------------------------------
PaymentwId(132655_02/12/2016_346039438,132655,02/12/2016,DFINE, Inc,90.87,Allopathic & Osteopathic Physicians|Radiology|Diagnostic Radiology,Food and Beverage)
PaymentwId(132655_02/13/2016_346039440,132655,02/13/2016,DFINE, Inc,23.45,Allopathic & Osteopathic Physicians|Radiology|Diagnostic Radiology,Food and Beverage)
PaymentwId(1006832_02/13/2016_346039442,1006832,02/13/2016,DFINE, Inc,32.0,Allopathic & Osteopathic Physicians|Radiology|Vascular & Interventional Radiology,Travel and Lodging)
...

18/05/02 21:09:49 ERROR JobScheduler: Error running job streaming job 1525295388000 ms.1
com.mapr.db.exceptions.DBException: tableExists() failed.,
        at com.mapr.db.exceptions.ExceptionHandler.handle(ExceptionHandler.java:65)
        at com.mapr.db.impl.AdminImpl.tableExists(AdminImpl.java:318)
        at com.mapr.db.impl.AdminImpl.tableExists(AdminImpl.java:307)
        at com.mapr.db.impl.MapRDBImpl.tableExists(MapRDBImpl.java:56)
        at com.mapr.db.spark.dbclient.DBOlderClientImpl$.tableExists(DBOlderClientImpl.scala:43)
        at com.mapr.db.spark.utils.MapRDBUtils$.checkOrCreateTable(MapRDBUtils.scala:24)
        at com.mapr.db.spark.RDD.DocumentRDDFunctions.saveToMapRDBInternal(DocumentRDDFunctions.scala:34)
        at com.mapr.db.spark.RDD.OJAIDocumentRDDFunctions.saveToMapRDB(DocumentRDDFunctions.scala:61)
        at com.mapr.db.spark.streaming.DStreamFunctions$$anonfun$saveToMapRDB$3.apply(DStreamFunctions.scala:25)
        at com.mapr.db.spark.streaming.DStreamFunctions$$anonfun$saveToMapRDB$3.apply(DStreamFunctions.scala:25)
        at org.apache.spark.streaming.dstream.DStream$$anonfun$foreachRDD$1$$anonfun$apply$mcV$sp$3.apply(DStream.scala:627)
        at org.apache.spark.streaming.dstream.DStream$$anonfun$foreachRDD$1$$anonfun$apply$mcV$sp$3.apply(DStream.scala:627)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1$$anonfun$apply$mcV$sp$1.apply$mcV$sp(ForEachDStream.scala:51)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1$$anonfun$apply$mcV$sp$1.apply(ForEachDStream.scala:51)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1$$anonfun$apply$mcV$sp$1.apply(ForEachDStream.scala:51)
        at org.apache.spark.streaming.dstream.DStream.createRDDWithLocalProperties(DStream.scala:415)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1.apply$mcV$sp(ForEachDStream.scala:50)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1.apply(ForEachDStream.scala:50)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1.apply(ForEachDStream.scala:50)
        at scala.util.Try$.apply(Try.scala:191)
        at org.apache.spark.streaming.scheduler.Job.run(Job.scala:39)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler$$anonfun$run$1.apply$mcV$sp(JobScheduler.scala:254)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler$$anonfun$run$1.apply(JobScheduler.scala:254)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler$$anonfun$run$1.apply(JobScheduler.scala:254)
        at scala.util.DynamicVariable.withValue(DynamicVariable.scala:58)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.run(JobScheduler.scala:253)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
Caused by: java.io.IOException: Could not create FileClient
        at com.mapr.fs.MapRFileSystem.lookupClient(MapRFileSystem.java:643)
        at com.mapr.fs.MapRFileSystem.lookupClient(MapRFileSystem.java:696)
        at com.mapr.fs.MapRFileSystem.getTableProperties(MapRFileSystem.java:3937)
        at com.mapr.db.impl.AdminImpl.tableExists(AdminImpl.java:313)
        ... 27 more
Caused by: java.io.IOException: Could not create FileClient
        at com.mapr.fs.MapRClientImpl.<init>(MapRClientImpl.java:136)
        at com.mapr.fs.MapRFileSystem.lookupClient(MapRFileSystem.java:637)
        ... 30 more
Exception in thread "main" com.mapr.db.exceptions.DBException: tableExists() failed.,
        at com.mapr.db.exceptions.ExceptionHandler.handle(ExceptionHandler.java:65)
        at com.mapr.db.impl.AdminImpl.tableExists(AdminImpl.java:318)
        at com.mapr.db.impl.AdminImpl.tableExists(AdminImpl.java:307)
        at com.mapr.db.impl.MapRDBImpl.tableExists(MapRDBImpl.java:56)
        at com.mapr.db.spark.dbclient.DBOlderClientImpl$.tableExists(DBOlderClientImpl.scala:43)
        at com.mapr.db.spark.utils.MapRDBUtils$.checkOrCreateTable(MapRDBUtils.scala:24)
        at com.mapr.db.spark.RDD.DocumentRDDFunctions.saveToMapRDBInternal(DocumentRDDFunctions.scala:34)
        at com.mapr.db.spark.RDD.OJAIDocumentRDDFunctions.saveToMapRDB(DocumentRDDFunctions.scala:61)
        at com.mapr.db.spark.streaming.DStreamFunctions$$anonfun$saveToMapRDB$3.apply(DStreamFunctions.scala:25)
        at com.mapr.db.spark.streaming.DStreamFunctions$$anonfun$saveToMapRDB$3.apply(DStreamFunctions.scala:25)
        at org.apache.spark.streaming.dstream.DStream$$anonfun$foreachRDD$1$$anonfun$apply$mcV$sp$3.apply(DStream.scala:627)
        at org.apache.spark.streaming.dstream.DStream$$anonfun$foreachRDD$1$$anonfun$apply$mcV$sp$3.apply(DStream.scala:627)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1$$anonfun$apply$mcV$sp$1.apply$mcV$sp(ForEachDStream.scala:51)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1$$anonfun$apply$mcV$sp$1.apply(ForEachDStream.scala:51)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1$$anonfun$apply$mcV$sp$1.apply(ForEachDStream.scala:51)
        at org.apache.spark.streaming.dstream.DStream.createRDDWithLocalProperties(DStream.scala:415)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1.apply$mcV$sp(ForEachDStream.scala:50)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1.apply(ForEachDStream.scala:50)
        at org.apache.spark.streaming.dstream.ForEachDStream$$anonfun$1.apply(ForEachDStream.scala:50)
        at scala.util.Try$.apply(Try.scala:191)
        at org.apache.spark.streaming.scheduler.Job.run(Job.scala:39)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler$$anonfun$run$1.apply$mcV$sp(JobScheduler.scala:254)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler$$anonfun$run$1.apply(JobScheduler.scala:254)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler$$anonfun$run$1.apply(JobScheduler.scala:254)
        at scala.util.DynamicVariable.withValue(DynamicVariable.scala:58)
        at org.apache.spark.streaming.scheduler.JobScheduler$JobHandler.run(JobScheduler.scala:253)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
Caused by: java.io.IOException: Could not create FileClient
        at com.mapr.fs.MapRFileSystem.lookupClient(MapRFileSystem.java:643)
        at com.mapr.fs.MapRFileSystem.lookupClient(MapRFileSystem.java:696)
        at com.mapr.fs.MapRFileSystem.getTableProperties(MapRFileSystem.java:3937)
        at com.mapr.db.impl.AdminImpl.tableExists(AdminImpl.java:313)
        ... 27 more
Caused by: java.io.IOException: Could not create FileClient
        at com.mapr.fs.MapRClientImpl.<init>(MapRClientImpl.java:136)
        at com.mapr.fs.MapRFileSystem.lookupClient(MapRFileSystem.java:637)
        ... 30 more