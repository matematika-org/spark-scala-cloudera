/**
 * Getting Technical with Spark
 *
 * This module starts covering the technical bits of Spark
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// *****************************************************************************************************
// *** SparkContext and SparkSession
//SparkContext
sc
sc.version
sc.getClass
sc.appName
sc.getClass
sc.getClass.getName
sc //hit tab, autocomplete will kick in
sc.uiWebUrl
sc.applicationId
sc.sparkUser
sc.textFile("/user/cloudera/spark-committers-no-header.tsv").take(10)
sc.parallelize(Array("Matei Zaharia", "Josh Rosen", "Holden Karau"))

//SparkSession
spark
spark.getClass.getName
spark.sparkContext
// For Spark SQL to work you need to create a temp view from a DataFrame, which can be done using the following command
//spark.read.option("delimiter", "\t").option("header", true).csv("/user/cloudera/spark-committers.tsv").createOrReplaceTempView("committers")
spark.sql("select * from committers").show(10)
// And to work with columns, which we will see in greater detail later
// val cmDF = spark.read.option("delimiter", "\t").option("header", true).csv("/user/cloudera/spark-committers.tsv")
cmDF.select("Name").show(10)
spark.catalog.listDatabases().show(truncate=false)


// *****************************************************************************************************
// *** Spark Configuration + Deployment Modes
// Configuration precedence
spark.conf.get("spark.eventLog.dir")

import org.apache.spark.sql.SparkSession
val spark_other_session = SparkSession.builder().
      master("yarn").
      appName("Word Count").
      config("spark.eventLog.dir", "/stackexchange/logs").
      getOrCreate()

spark_other_session.conf.get("spark.eventLog.dir")

// Start shell and pass paramters
// # pyspark2 --name 'Super PySparkShell' --conf 'spark.eventLog.dir=/stackexchange/logs/'
sc.appName
:q
// # ls /stackexchange/logs
// # cat /stackexchange/logs/application_1511794877761_0020

// SparkConf with RDD API
// Required to stop current context before creating a new one
// sc.stop
import org.apache.spark.{SparkContext, SparkConf}

val conf = new SparkConf().
     setMaster("yarn").
     setAppName("Stack Overflow Test").
     set("spark.executor.memory", "1g")

val sc = new SparkContext(conf)

// SparkConf with spark2-submit
// # spark2-submit --executor-memory 4g stackoverflowtest.scala

// Reviewing Configuration
sc.getConf.getAll
sc.getConf.get("spark.driver.host")

// SparkConf with Dataset API
import org.apache.spark.sql.SparkSession
val spark = SparkSession.
   builder().
   appName("StackOverflow Test").
   config("spark.executor.memory","1g").
   getOrCreate()

// Client or Cluster deployment mode
val spark = SparkSession.
   builder().
   appName("StackOverflow Test").
   config("spark.submit.deployMode","client").
   getOrCreate()

val spark = SparkSession.
   builder().
   appName("StackOverflow Test").
   config("spark.submit.deployMode","cluster").
   getOrCreate()


// *****************************************************************************************************
// *** Visualizing Your Spark App: Web UI and History Server
sc.getConf.get("spark.driver.appUIAddress")

// alpharetta is the name of the virtual host machine where I have my cluster
// Spark Web UI
// http://alpharetta:4040

// Spark History Server
// http://alpharetta:18089


// *****************************************************************************************************
// *** Logging in Spark and with Cloudera
sc.setLogLevel("ALL")

// Use the inExecutorEntryLogLevelAll.scala and inExecutorEntryNoLogLevel.scala for playing around with log files

// *****************************************************************************************************
// *** Navigating the Spark Documentation
// https://spark.apache.org/
// https://spark.apache.org/docs/latest/api/scala/index.html
// https://www.cloudera.com/products/open-source/apache-hadoop/apache-spark.html