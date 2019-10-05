/**
 * Increasing Proficiency with Spark: DataFrames & Spark SQL
 *
 * This module covers DataFrames & Spark SQL
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// *****************************************************************************************************
// *** SparkSession: The Entry Point to the Spark SQL / DataFrame API ***
// Creating a new Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.master("yarn")
                        .appName("StackOverflowTest")   
                        .config("spark.executor.memory", "2g")
                        .getOrCreate()

// Test bundling and running both from terminal
// # cd no_session
// # sbt package

// # cd with_session
// # sbt package
// # spark2-submit target/scala-2.11/with-session_2.11-1.0.jar

// Create a new session
spark
val spark_two = spark.newSession()
spark_two


// *****************************************************************************************************
// *** Creating and Loading DataFrames - Including Schemas***
// Create a dataframe manually, from a list
val qa_listDF =  spark.createDataFrame(Array((1,"Xavier"),(2,"Irene"),(3,"Xavier")))
qa_listDF.getClass

// Remember collect? You can use to return data to the Driver program
qa_listDF.collect()
// Compare with RDD
sc.parallelize(Array((1,"Xavier"),(2,"Irene"),(3,"Xavier"))).collect()

// Collect works, but there is a better way
qa_listDF.collect()
qa_listDF.take(3)
qa_listDF.show()
qa_listDF.show(1)

// More options for returning data to the driver
qa_listDF.limit(1)
qa_listDF.limit(1).show()
qa_listDF.head()
qa_listDF.first()
qa_listDF.take(1)
qa_listDF.sample(false, .3, 42).collect()

// Nicer column names
qa_listDF.show()
val qaDF = qa_listDF.toDF("Id", "QA")
qaDF.show()


// *****************************************************************************************************
// *** DataFrames to RDDs and Viceversa ***
val qa_rdd = sc.parallelize(Array((1,"Xavier"), (2,"Irene"), (3,"Xavier")))
val qa_with_ToDF = qa_rdd.toDF()
val qa_with_create = spark.createDataFrame(qa_rdd)
qa_rdd.collect()
qa_with_ToDF.show()
qa_with_create.show()
// To RDD
qa_rdd.collect()
qa_with_ToDF.rdd.collect()
qa_with_ToDF.rdd.map(x => (x(0), x(1))).collect()


// badges_columns_rdd was loaded in a previous module, but just in case, here is what you need
// Load CSV when using textFile has caveats
// def split_the_line(x: String): Array[String] = x.split(",")
// val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
// val badges_columns_rdd = badges_rdd_csv.map(split_the_line)
badges_columns_rdd.take(3)
val badges_from_rddDF = badges_columns_rdd.toDF()
badges_from_rddDF.show(5)
val badges_tuple = badges_columns_rdd.map(x => (x(0), x(1), x(2), x(3), x(4), x(5))).toDF()
badges_tuple.show(5)

// # This runs in terminal first, then inside MySQL. Intention is to show how similar the output of a table is with .show()
// # mysql -u root -p
// # show databases;
// # use scm;
// # show tables;
// # Select PRODUCT, VERSION FROM PARCELS;

// And now back to DataFrame, we can check the schema and see that we have an array 
badges_from_rddDF.printSchema()
badges_tuple.printSchema()


// *****************************************************************************************************
// *** Loading DataFrames: Text and CSV ***
// Load with Format text
val posts_no_schemaTxtDF = spark.read.format("text").load("/user/cloudera/stackexchange/posts_all_csv")
posts_no_schemaTxtDF.printSchema()
posts_no_schemaTxtDF.show(5)
posts_no_schemaTxtDF.show(5, truncate=false)

// Drowning in data 
posts_no_schemaTxtDF.show(5, truncate=false)

// Specifying format directly
val posts_no_schemaTxtDF = spark.read.text("/user/cloudera/stackexchange/posts_all_csv")
posts_no_schemaTxtDF.printSchema()
posts_no_schemaTxtDF.show(2)


// Read csv
val posts_no_schemaCSV = spark.read.csv("/user/cloudera/stackexchange/posts_all_csv")
posts_no_schemaCSV.show(5)
posts_no_schemaCSV.printSchema()


// *****************************************************************************************************
// *** Schemas: Inferred and Programatically Specified + Option ***
// You can ask Spark to infer the schema
val posts_inferred = spark.read.option("inferSchema","true").csv("/user/cloudera/stackexchange/posts_all_csv")
posts_inferred.printSchema()

// Spark reads ahead
val an_rdd = sc.textFile("/thisdoesnotexist")
val a_df = spark.read.text("/thisdoesnotexist")

// There are multiple ways of loading the data
spark.read.csv("/user/cloudera/stackexchange/posts_all_csv").printSchema
spark.read.option("inferSchema","true").csv("/user/cloudera/stackexchange/posts_all_csv").printSchema
spark.read.option("inferSchema","true").option("sep", "|").csv("/user/cloudera/stackexchange/posts_all_csv").printSchema
spark.read.options(Map("inferSchema"->"true", "sep"->"|")).csv("/user/cloudera/stackexchange/posts_all_csv").printSchema

// Get column headers
// Requires posts csv with header
posts_inferred.printSchema()
val posts_headersDF = spark.read.option("inferSchema", "true").option("header", true).csv("/user/cloudera/stackexchange/posts_all_csv_with_header")
posts_headersDF.printSchema()
posts_headersDF.show(5)

// Or directly create the schema
import org.apache.spark.sql.types._

/**
// Incorrect
val postsSchema = 
  StructType(Array(
              StructField("Id", IntegerType),
StructField("PostTypeId", IntegerType),
StructField("AcceptedAnswerId", IntegerType),
StructField("CreationDate", TimestampType),
StructField("Score", IntegerType),
StructField("ViewCount", StringType),
StructField("OwnerUserId", IntegerType),
StructField("LastEditorUserId", IntegerType),
StructField("LastEditDate", TimestampType),
StructField("Title", StringType),
StructField("LastActivityDate", TimestampType),              
StructField("Tags", StringType),
StructField("AnswerCount", IntegerType),
StructField("CommentCount", IntegerType),
StructField("FavoriteCount", IntegerType)))
*/

// Correct
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

val postsDF = spark.read.schema(postsSchema).csv("/user/cloudera/stackexchange/posts_all_csv")
postsDF.printSchema()
postsDF.schema
postsDF.dtypes
postsDF.columns


//  *****************************************************************************************************
//# *** More Data Loading: Parquet and JSON ***
val default_formatDF = spark.read.load("/user/cloudera/stackexchange/posts_all_csv")

// Loading Parquet
val comments_parquetDF = spark.read.parquet("/user/cloudera/stackexchange/comments_parquet")
comments_parquetDF.printSchema()
comments_parquetDF.show(5)

// Loading JSON
val tags_jsonDF = spark.read.json("/user/cloudera/stackexchange/tags_json")
tags_jsonDF.printSchema()
comments_parquetDF.show(5)


// *****************************************************************************************************
// *** Rows, Columns, Expressions and Operators ***
// Let's inspect one row
postsDF.take(1)
postsDF.show(1)
tags_jsonDF.show(5)
postsDF.columns


// *****************************************************************************************************
// *** Working with Columns ***
// Refer to columns
postsDF['Title']
$"Title"
col("Title") 

// And we can return the values in those columns
postsDF.select(postsDF("Title")).show(1)
postsDF.select($"Title").show(1)
postsDF.select(col("Title")).show(1)


// Specify multiple columns, 
postsDF.select($"Title", postsDF("Id"), col("CreationDate").show(1)
// But you can't mix strings with column references - this will raise an error
postsDF.select("Title", $"CreationDate").show(1)

// Specify a function or do some math
postsDF.select($"Score" * 1000).show(1)
// But you need to use a column reference. With a string you get an error
postsDF.select("Score" * 1000).show(1)

// When using a string for an operation, you need to use lit
import org.apache.spark.sql.functions.{col,lit,concat}
// This won't work
postsDF.select(concat("Title: ", $"Title")).show(1)
// You need to use 
postsDF.select(concat(lit("Title: "), $"Title")).show(1)


// *****************************************************************************************************
// *** More Columns, Expressions, Cloning, Renaming, Casting & Dropping ***
val postsSchema = StructType(Array(
    StructField("Id", IntegerType),
    StructField("PostTypeId", IntegerType),
    StructField("AcceptedAnswerId", IntegerType),
    StructField("CreationDate", TimestampType),
    StructField("Score", IntegerType),
    StructField("ViewCount", StringType),
    StructField("OwnerUserId", IntegerType),
    StructField("LastEditorUserId", IntegerType),
    StructField("LastEditDate", TimestampType),
    StructField("Title", StringType),
    StructField("LastActivityDate", TimestampType), 
    StructField("Tags", StringType),
    StructField("AnswerCount", IntegerType),
    StructField("CommentCount", IntegerType),
    StructField("FavoriteCount", IntegerType)))

// Casting columns
postsDF.dtypes
postsDF.dtypes.find(x => x._1 == "ViewCount")
postsDF.schema("ViewCount")
postsDF.select("ViewCount").printSchema()
val posts_viewDF = postsDF.withColumn("ViewCount", postsDF("ViewCount").cast("Integer"))
posts_viewDF.select("ViewCount").printSchema()
posts_viewDF.select("ViewCount").show(5)	
	
// We can rename a column
val postsVCDF = postsDF.withColumnRenamed("ViewCount", "ViewCountStr")
postsVCDF.printSchema()

// Rename two columns
val posts_twoDF = postsDF.withColumnRenamed("ViewCount", "ViewCountStr").withColumnRenamed("Score", "ScoreInt")
posts_twoDF.printSchema()

// Some columns might cause problems, you need to escape them wit back ticks
val posts_ticksDF = postsDF.withColumnRenamed("ViewCount", "ViewCount.Str")
// This raises an exception
posts_ticksDF.select("ViewCount.Str").show(3)

// Escape with back ticks, this works
posts_ticksDF.select("`ViewCount.Str`").show()

// Copy columns, several ways to do it, sometimes string, sometimes col objects
val posts_wcDF = postsDF.withColumn("TitleClone1", $"Title") 
posts_wcDF.printSchema()
postsDF.withColumn("Title", concat(lit("Title: "), $"Title")).select($"Title").show(5)

// ?? import org.apache.spark.sql.functions.lower
postsDF.withColumn("Title", lower($"Title")).select("Title").show(5)

// Drop a column
posts_wcDF.columns
posts_wcDF.columns.contains("TitleClone1")
val posts_no_cloneDF = posts_wcDF.drop("TitleClone1")
posts_no_cloneDF.columns.contains("TitleClone1")
posts_no_cloneDF.printSchema()

 
// *****************************************************************************************************
// ***  User Defined Functions (UDFs) on Spark SQL ***
// Change a type on a column
// Check out the triple equal sign for comparison, this is important
val questionsDF = postsDF.filter(col("PostTypeId") === 1)
questionsDF.select("Tags").show(5, truncate=false)
questionsDF.select("Tags").take(1)(0)(0).getClass


// Define a column based function
def give_me_list(str_lst: String): List[String] = {
    if(str_lst == "()" || Option(str_lst).getOrElse("").isEmpty || str_lst.length < 2) return List[String]()    
    val elements = str_lst.slice(1, str_lst.length - 1)
    elements.split(",").toList
  }

val list_in_string = "(indentation,line-numbers)"
give_me_list(list_in_string)

import org.apache.spark.sql.functions.udf
val udf_give_me_list = udf(give_me_list _)

val questions_id_tagsDF = questionsDF.withColumn("Tags",udf_give_me_list(questionsDF("Tags"))).select("Id", "Tags")

questions_id_tagsDF.printSchema()
questions_id_tagsDF.select("Tags").show(10, truncate=false)

// Get tags exploded
import org.apache.spark.sql.functions.explode
questions_id_tagsDF.select(explode(questions_id_tagsDF("Tags"))).show(10)
questions_id_tagsDF.select(explode(questions_id_tagsDF("Tags"))).count()
questions_id_tagsDF.select(explode(questions_id_tagsDF("Tags"))).distinct().count()