/**
 * Learning the Core of Spark: RDDs
 *
 * This module starts covering the lower level Spark API: RDDs
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// ***************************************************************************************
// *** SparkContext: The Entry Point to a Spark Application ***
// # spark2-shell
sc
spark.sparkContext
sc.sparkUser
sc.stop
sc
val test_rdd = sc.emptyRDD
test_rdd.collect()
import org.apache.spark.SparkContext
val sc = SparkContext.getOrCreate()
test_rdd = sc.emptyRDD
test_rdd.collect()


// ***************************************************************************************
// *** Creating RDDs with Parallelize ***
// Parallelize is used to create RDDs, usually for testing or to operate with larger RDDs
// Here is a simple RDD with just five elements, I can see how many partitions are created
List(1,2,3,4,5)
val list_one_to_five = sc.parallelize(List(1,2,3,4,5))
list_one_to_five
list_one_to_five.collect()

// And you can specify how many partitions you want
list_one_to_five.getNumPartitions
list_one_to_five.glom().collect()
val list_one_to_five = sc.parallelize(List(1,2,3,4,5), 1)
list_one_to_five.getNumPartitions
list_one_to_five.glom().collect()

// Perform operations
list_one_to_five.sum()
list_one_to_five.min()
list_one_to_five.max()
list_one_to_five.mean()
list_one_to_five.first()

// RDDs can hold objects of different types, key/value pairs being common (tuple)
val list_dif_types = sc.parallelize(Seq(false, 1, "two", Map("three" -> 3), ("xavier", 4)))
list_dif_types.collect()
val tuple_rdd = sc.parallelize(Seq(("xavier", 1), ("irene", 2)))
tuple_rdd.setName("tuple_rdd")
tuple_rdd

// RDDs can be empty too, and you can check if they are
val empty_rdd = sc.parallelize(Array[String]())
empty_rdd.collect()
empty_rdd.isEmpty()
val other_empty = sc.emptyRDD
other_empty.isEmpty()

// We can create a longer list and play around with partitions. 
val bigger_rdd = sc.parallelize(1 to 1000)
bigger_rdd.collect()
bigger_rdd.sum()
bigger_rdd.count()
bigger_rdd.getNumPartitions


// ***************************************************************************************
// *** Returning Data to the Driver, i.e. collect(), take(), first()... ***
// Many methods to bring back data
bigger_rdd.collect()
bigger_rdd.take(10)
bigger_rdd.first()
bigger_rdd.takeOrdered(10)(Ordering[Int].reverse)

val tuple_map = tuple_rdd.collectAsMap()
tuple_map("xavier")

// And iterate over the data
for (elem <- bigger_rdd.take(10)) println(elem)

// And over the map
for ((k, v) <- tuple_map)
  println(k + " / " + v)

// Foreach runs method on each element in RDD
def log_search(url: String) = {
  val page = scala.io.Source.fromURL("http://tiny.bigdatainc.org/" + url)
  print("Url Accessed" + "http://tiny.bigdatainc.org/" + url)
}

val queries = sc.parallelize(Seq("ts1", "ts2"))
queries.foreach(log_search)


// ***************************************************************************************
// *** Partitions, Repartition, Coalesce, Saving as Text and HUE ***
bigger_rdd.getNumPartitions
val play_part = bigger_rdd.repartition(10)
play_part.getNumPartitions

// But try to increase number of partitions with both repartition and coalesce
// Coalesce can only be used to decrease partitions, and it has the advantage that it minimizes data movement as explained in the video. Repartition works as indicated.
play_part.repartition(14).getNumPartitions
play_part.coalesce(15).getNumPartitions

// But how do we really know the data is being partitioned? Save the RDD and visualize in HUE
play_part.saveAsTextFile("/user/cloudera/tests/play_part/ten")
// Now try with repartition(), increasing the number of partitions and visualize in HUE
play_part.repartition(4).saveAsTextFile("/user/cloudera/tests/play_part/four")
// Now try with coalesce(), we will get 1 file
play_part.coalesce(1).saveAsTextFile("/user/cloudera/tests/play_part/coalesce")
// You can't overwrite
play_part.coalesce(1).saveAsTextFile("/user/cloudera/tests/play_part/coalesce")


// ************************************************************************************************
// *** Creating RDDs from External Datasets ***
sc.textFile("/user/cloudera/tests/play_part/ten").count()
sc.textFile("/user/cloudera/tests/play_part/four").count()
sc.textFile("/user/cloudera/tests/play_part/coalesce").count()
sc.textFile("/user/cloudera/tests/play_part/four/part-00000").count()

// You can also load locally - note: Spark's behavior is different between Yarn and Standalone in term of defaults
// Upload the file to only one machine when working in a cluster, you will get a file not found exception (in standalone it will not make a difference)
// Upload to all machines and you will see it working
val local_play = sc.textFile("file:///stackexchange/play_part/")
local_play.count
local_play.take(10)

// Let's load our StackOverflow posts
val posts_all = sc.textFile("/user/cloudera/stackexchange/posts_all_csv")
posts_all.count()

// Load CSV when using textFile has caveats
def split_the_line(x: String): Array[String] = x.split(",")

val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
badges_rdd_csv.take(1)
badges_rdd_csv.take(1)(0)

val badges_columns_rdd = badges_rdd_csv.map(split_the_line)
badges_columns_rdd.take(1)(0)(2)

// Loading whole text files
val numbers_partitions = sc.wholeTextFiles("/user/cloudera/tests/play_part/four")

numbers_partitions.take(1)
numbers_partitions.take(1)(0)._1

// You can load JSON files as well
val tags_json = sc.textFile("/user/cloudera/stackexchange/tags_json")
tags_json.first()

// And you can also upload to S3, you need to set your access key id and secret (there are more secure ways of doing this)
val tags_json_s3 = sc.textFile("s3a://pluralsight-spark-cloudera-scala/part-00000")
tags_json_s3.take(10) 

/**
 There are a couple of steps, you need to load the Hadoop and AWS libraries, it can be done with the steps below
   spark2-shell --packages org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4
 You then need to set the access key and secret access key, there are several ways. You can set the access and secret key, it has to be done in all nodes
   export AWS_ACCESS_KEY_ID="access-key"
   export AWS_SECRET_ACCESS_KEY="secret-key"
 Another possibility is to use jceks 
   hadoop credential create fs.s3a.access.key -provider jceks://hdfs/user/root/awskeyfile.jceks -value <accesskey>
   hadoop credential create fs.s3a.secret.key -provider jceks://hdfs/user/root/awskeyfile.jceks -value <secretkey>
   spark2-shell --conf spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs/user/root/awskeyfile.jceks    
 You could also set using configuration
   sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", "<accesskey>")
   sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", "<secretkey>")
 Or in core-site.xml using the Safety Valve from Cloudera Manager
 There are many ways, some more secure than others. Please check Cloudera's documentation for more information on setting access keys
 Additional note: please make sure ntp service is running and there is no clock offset issue. This may cause a 403 forbidden access error.
*/


// ************************************************************************************************
// *** Saving Data as ObjectFile, NewAPIHadoopFile, SequenceFile, .... ***
// textFile is the most common way of reading, however is not the most convenient. Let's learn about a few other options 
badges_columns_rdd.take(1)
badges_columns_rdd.take(1)(0)(2)
badges_columns_rdd.saveAsTextFile("/user/cloudera/stackexchange/badges_txt_array")
badges_columns_rdd.map(x => x.mkString(",")).saveAsTextFile("/user/cloudera/stackexchange/badges_txt")

// Reload and see what happens
val badges_reloaded = sc.textFile("/user/cloudera/stackexchange/badges_txt")
badges_reloaded.take(1)
badges_reloaded.take(1)(0)
badges_reloaded.take(1)(0)(0)

badges_columns_rdd.take(1)
badges_columns_rdd.take(1)[0][2]

// Use object files
badges_columns_rdd.saveAs. //hit tab
badges_columns_rdd.saveAsObjectFile("/user/cloudera/stackexchange/badges_object")
val badges_object = sc.objectFile[Array[String]]("/user/cloudera/stackexchange/badges_object")
badges_object.take(1)

// SequenceFile
tuple_rdd.saveAsSequenceFile("/user/cloudera/stackexchange/tuple_sequence")

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
sc.sequenceFile("/user/cloudera/stackexchange/tuple_sequence", classOf[Text], classOf[Longritable]).collect()

// Use Hadoop Input and Output formats
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.io._

val prepareForNAH: Array[String] => (Text, Text) = x => (new Text(x(0)), new Text(x(2)))
badges_columns_rdd.map(prepareForNAH).saveAsNewAPIHadoopFile("/user/cloudera/stackexchange/badgess_newapihadoop", classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text, Text]])

//Read
val badges_newapihadoop = sc.newAPIHadoopFile("/user/cloudera/stackexchange/badgess_newapihadoop", classOf[SequenceFileInputFormat[Text, Text]], classOf[Text], classOf[Text])


// ************************************************************************************************
// *** Creating RDDs with Transformations ***
val rdd_reuse = sc.parallelize(Array(1,2))
rdd_reuse.collect()
val rdd_reuse = sc.parallelize(Array(3,4))
rdd_reuse.collect()

// Now let's say that we want to create several RDDs with the end goal of counting the badges
val badges_entry = badges_columns_rdd.map(x => x(2))
badges_entry.take(1)

val badges_name = badges_entry.map(x => (x, 1))
badges_name.take(1)

val badges_reduced = badges_name.reduceByKey(_ + _)
badges_reduced.take(1)

val badges_count_badge = badges_reduced.map({ case (x,y) => (y,x) })
badges_count_badge.take(1)

val badges_sorted = badges_count_badge.sortByKey(false).map({ case (x, y) => (y, x) })
badges_sorted.take(10)
badges_sorted.take(1)

// You can list RDDs
$intp.definedTerms.map(defTerms => s"${defTerms.toTermName}:   ${$intp.typeOfTerm(defTerms.toTermName.toString)}").filter(x => x.contains("()org.apache.spark.rdd.RDD")).foreach(println)

		
// ************************************************************************************************
// *** A Little Bit More on Lineage ***
// To see RDD Lineage
badges_sorted
badges_sorted.toDebugString
// At this point go check the details for this job in the Spark UI, you will see how they match and the shuffle boundaries are pretty obvious

// And you can explore dependencies
badges_sorted.toDebugString
badges_reduced.dependencies