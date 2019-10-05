/**
 * Understanding a Typed API: Datasets
 *
 * This module covers the typed higher-level API, Datasets
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// *****************************************************************************************************
// *** The Motivation Behind Datasets ***
// Create an RDD and a DS with the same name
val postsRDD = sc.textFile("/user/cloudera/stackexchange/simple_titles_txt")
val postsDS = spark.read.text("/user/cloudera/stackexchange/simple_titles_txt").as[String]

// Let's name the RDD, for DataFrames please wait for JIRA SPARK-8480
postsRDD.setName("postsRDD")

// Let's cache
postsRDD.cache
postsDS.cache

// And call an action, now compare sizes in memory. "Wow'ed" at Datasets?
// That is KB, but imagine GB or TB!
postsRDD.count
postsDS.count

// What about DFs? Let's check the size
val postsDF = spark.read.text("/user/cloudera/stackexchange/simple_titles_txt")
postsDF.cache
postsDF.count

// Let's prepare a Dataset and a DataFrame for testing 
import org.apache.spark.sql.types._
val postsSchema = 
  StructType(Array(
              StructField("Id", IntegerType),
StructField("PostTypeId", IntegerType),
StructField("AcceptedAnswerId", IntegerType),
StructField("CreationDate", TimestampType),
StructField("Score", IntegerType),
StructField("ViewCount", IntegerType),
StructField("OwnerUserId", IntegerType),
StructField("LastEditorUserId", IntegerType),
StructField("LastEditDate", TimestampType),
StructField("Title", StringType),
StructField("LastActivityDate", TimestampType),              
StructField("Tags", StringType),
StructField("AnswerCount", IntegerType),
StructField("CommentCount", IntegerType),
StructField("FavoriteCount", IntegerType)))

case class Post(Id: Int, PostTypeId: Int, Score: Integer, ViewCount: Integer, AnswerCount: Integer, OwnerUserId: Integer)

val posts_all = spark.read.schema(postsSchema).csv("/user/cloudera/stackexchange/posts_all_csv")

val postsDF = posts_all.select($"Id", $"PostTypeId", $"Score", $"ViewCount", $"AnswerCount", $"OwnerUserId").na.drop("any", Seq("ViewCount"))
val postsDS = posts_all.select($"Id", $"PostTypeId", $"Score", $"ViewCount", $"AnswerCount", $"OwnerUserId").as[Post]


postsDF.first. // tab
postsDS.first. // tab 

// For errors at runtime vs. compile time
/**
postsDF.createOrReplaceTempView("PostsSE")
*/

// SQL
// Syntax error
spark.sql("Selct Score from PostsSE").show(5)
// Analysis error
spark.sql("Select Scre from PostsSE").show(5)

// DataFrames
// Syntax error
postsDF.selct("Score").show(5)
// Analysis error
postsDF.select($"Scre").first

// Datasets
// Syntax error
postsDS.first.Scre
// Analysis Error
val firstScore: String = postsDS.first.Score


// *****************************************************************************************************
// *** What Do You Need for Datasets? ***

// You need a case class
case class Post(Id: Integer, UserId: String, Score: Integer)
val post = Post(1, "1", 25)
post.Id
post.UserId
post.UserId = "2"


// *****************************************************************************************************
// *** Creating Datasets ***

val primitiveDS = Seq(10, 20, 30).toDS()
val differentDS = Seq(10, "20", 30).toDS()
primitiveDS.map(_ + 1).show()

val complexDS = Seq(("Xavier", 1), ("Irene", 2)).toDS()
complexDS.filter(x => x._1 == "Xavier").show()

// From RDDs
val postsRDD = sc.textFile("/user/cloudera/stackexchange/simple_titles_txt")
val postsDSfromRDD = spark.createDataset(postsRDD)
postsDSfromRDD.show(5)

// From DataFrames
val postsDSfromDF = postsDF.as[Post]

// And now we can do things like
postsDSfromDF.groupByKey(row => row. // hit tab

postsDSfromDF.groupByKey(row => row.OwnerUserId).count().show()


// *****************************************************************************************************
// *** Dataset Operations ***
// Typed transformation
postsDS
val postsLessDS = postsDS.filter('ViewCount < 533)
postsLessDS
 
// Untyped transformation
val postsNotDS = postsDS.select('Id, 'ViewCount)
postsNotDS

// Several ways of creating datasets
postsDS.describe("ViewCount").show()
postsDS.filter(p => (p.ViewCount == 533)).show()
postsDS.filter(p => p.OwnerUserId == 51).count()
 
// Explore the API
val mini_postsDS = postsDS.map(d => (d.Id, d.Score))
val mini_postsDF = postsDS.select($"Id", $"Score")
postsDS.groupBy($"UserId")
postsDS.groupBy($"UserId"). //tab