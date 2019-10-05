/**
 * Continuing the Journey with Spark SQL
 *
 * This module continues on the journey with DataFrames & Spark SQL
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// *****************************************************************************************************
// *** Querying DataFrames (And Sorting, Alias, ...) ***
// postsDF created in a previous module
postsDF.show()

// Here is how I select the column, I get a new DataFrame
postsDF.select("Id")
postsDF.select("Id").show()
postsDF.select("Id").show(5)
postsDF.select("Id").limit(5).show()
postsDF.select("Id", "Title").show(5)

// Different notations
postsDF.select("Id").show(1)
postsDF.select("Id", "Title").show(1)
// This should fail
postsDF.select("Id", "Title", $"Score").show(1)
postsDF.select(col("Id"), postsDF("Title"), $"Score", $"CreationDate").show(1)
postsDF.select(col("Id"), postsDF("Title"), $"Score" * 1000, $"CreationDate").show(1)

// Filtering Data, use where() and filter()
// filter and where are aliases, use three equal signs
postsDF.filter(col("PostTypeId") === 1).count()
postsDF.where(col("PostTypeId") === 1).count()
postsDF.select(col("PostTypeId")).distinct().show()

// Evaluationg conditions
postsDF.select($"Id", when(col("PostTypeId") === 1, "Question").otherwise("Other"), $"Title").show(5)
postsDF.select($"Id", when(col("PostTypeId") === 1, "Question").otherwise("Other").alias("PostType"), $"Title").show(5)
postsDF.select($"Id", when(col("PostTypeId") === 1, "Question").when(col("PostTypeId") === 2, "Answer").otherwise("Other").alias("PostType"), $"Title").show(100)

// Multiple conditions in an expressions
postsDF.where((col("PostTypeId") === 5) || (col("PostTypeId") === 1)).count()
postsDF.where((col("PostTypeId") === 5) && (col("PostTypeId") === 1)).count()

// Ordering results 
val qDF = questionsDF.select("Title", "AnswerCount", "Score")
qDF.orderBy("AnswerCount").show(5)
qDF.orderBy($"AnswerCount".desc).show(5)
qDF.sort($"AnswerCount".desc).show(5)

// *****************************************************************************************************
// *** What To Do With Missing or Corrupt Data ***
// Removing nulls
postsDF.select("Id", "Title").show(10)
postsDF.select("Id", "Title").na.drop().show(10)

postsDF.select("Id", "Title").na.drop("any").show(10)
postsDF.select("Id", "Title").na.drop("all").show(10)
postsDF.select("Id", "Title").na.drop("any", Seq("Title")).show(10)

// Replacing & Filling
postsDF.select("Id", "Title").show(5)
postsDF.select("Id", "Title").na.replace(Seq("Title"), Map("How can I add line numbers to Vim?" -> "[Redacted]")).show(5)
postsDF.select("Id", "Title").na.fill("[N/A]").show(5)

// Handling bad records
// badges_columns_rdd was created in a previous module
// def split_the_line(x: String): Array[String] = x.split(",")
// val badges_rdd_csv = sc.textFile("/user/cloudera/stackexchange/badges_csv")
// val badges_columns_rdd = badges_rdd_csv.map(split_the_line)

val badges_for_JSON = badges_columns_rdd.map(x => (x(0), x(1), x(2), x(3), x(4), x(5)))
val badgeColumns = Seq("Id", "UserId", "Name", "Date", "BadgeClass", "TagBase")
badges_for_JSON.toDF(badgeColumns: _*).write.json("/user/cloudera/stackexchange/badges_records")
// Here I corrupt manually the first record
val badgesDF = spark.read.json("/user/cloudera/stackexchange/badges_records")
badgesDF.show(5)

// Handling corrupt data
spark.read.option("mode", "PERMISSIVE").json("/user/cloudera/stackexchange/badges_records").show(5)
spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "Invalid").json("/user/cloudera/stackexchange/badges_records").show(5)
spark.read.option("mode", "DROPMALFORMED").json("/user/cloudera/stackexchange/badges_records").show(5)
spark.read.option("mode", "FAILFAST").json("/user/cloudera/stackexchange/badges_records").show(5)


// *****************************************************************************************************
// *** Saving DataFrames ***
// Nothing specified, saves as parquet, and I can see there is only 1 partition
postsDF.write.save("/user/cloudera/stackexchange/dataframes/just_save")
postsDF.rdd.getNumPartitions

// I can repartition my dataframe and save, now I will get two partitions
postsDF.repartition(2).write.save("/user/cloudera/stackexchange/dataframes/two_partitions")

// I can save as text. If you want to save as text, it has to be text, not columnar data.
postsDF.write.format("text").save("/user/cloudera/stackexchange/dataframes/just_text")

// Now save only 1 column
postsDF.select("Title").write.format("text").save("/user/cloudera/stackexchange/dataframes/just_text_title")

// Although you can use text, but in a columnar format, namely CSV
postsDF.write.format("csv").save("/user/cloudera/stackexchange/dataframes/format_csv")
postsDF.write.csv("/user/cloudera/stackexchange/dataframes/just_csv")

// Output options
postsDF.write.option("header", "true").csv("/user/cloudera/stackexchange/dataframes/csv_h")

// And there are savemodes, with RDDs you cannot overwrite
postsDF.rdd.saveAsTextFile("/user/cloudera/stackexchange/dataframes/just_rdd")
postsDF.rdd.saveAsTextFile("/user/cloudera/stackexchange/dataframes/just_rdd")

// But it is possible with dataframes
postsDF.write.csv("/user/cloudera/stackexchange/dataframes/repeat_df")
postsDF.write.csv("/user/cloudera/stackexchange/dataframes/repeat_df")

postsDF.write.mode("overwrite").csv("/user/cloudera/stackexchange/dataframes/repeat_df")
postsDF.write.mode("overwrite").csv("/user/cloudera/stackexchange/dataframes/repeat_df")
postsDF.write.mode("overwrite").csv("/user/cloudera/stackexchange/dataframes/repeat_df")


// *****************************************************************************************************
// *** Spark SQL: Querying Using Temporary Views ***
// Now just use SQL
spark.sql("select * from PostsDF").show()

// Temporary Views in Spark SQL
postsDF.createOrReplaceTempView("Posts")
spark.sql("select count(*) from Posts")
spark.sql("select count(*) from Posts").show()
spark.sql("select count(*) as TotalPosts from Posts").show()
spark.sql("select Id, Title, ViewCount, AnswerCount from Posts").show(5, truncate=false)

// And also use the Spark SQL functions
val top_viewsDF = spark.sql("select Id, reverse(Title) as eltiT, ViewCount, AnswerCount from Posts order by ViewCount desc")
top_viewsDF.select("eltiT", "ViewCount").show(5, truncate=false)


// *****************************************************************************************************
// *** Loading Files and Views into DataFrames Using Spark SQL ***
val comments_loadedDF = spark.sql("select * from parquet.`/user/cloudera/stackexchange/comments_parquet`")
comments_loadedDF.show(5)

// And you can run queries
spark.sql("select * from parquet.`/user/cloudera/stackexchange/comments_parquet` order by Score desc").show(5)

// Load View as DataFrame
comments_loadedDF.createOrReplaceTempView("comments")

// And load using table
val comments_reloadedDF = spark.table("comments")
comments_reloadedDF.orderBy($"score".desc).show(5) 


// *****************************************************************************************************
// *** Saving to Persistent Tables ***

postsDF.write.saveAsTable("posts_savetable_nooption")
spark.sql("select * from posts_savetable_nooption")
spark.sql("select * from posts_savetable_nooption").show()

postsDF.write.option("path", "/user/cloudera/stackexchange/tables").saveAsTable("posts_savetable_option")
spark.sql("select * from posts_savetable_option").show(3)

// Please see known issue
// https://www.cloudera.com/documentation/spark2/latest/topics/spark2_known_issues.html#SPARK-21994

// *****************************************************************************************************
// ***  Hive Support and External Databases ***
/** You can connect to a database
ls -l mysql-connector-java-5.1.41-bin.jar
spark2-shell --jars mysql-connector-java-5.1.41-bin.jar
*/

 val external_databaseDF = spark.read.format("jdbc").
     option("url", "jdbc:mysql://dn04/cloudera/scm").
     option("driver", "com.mysql.jdbc.Driver").
     option("dbtable", "COMMANDS").
     option("user", "scm_user").
     option("password", "scm_pwd").
     load()

external_databaseDF.count()
external_databaseDF.columns
external_databaseDF.show(5)
 
// Or to Hive
// warehouse_location points to the default location for managed databases and tables
// Configure Hive as Service wide from configuration in Cloudera Manager

import org.apache.spark.sql.SparkSession

val spark_hive = SparkSession.builder.appName("Hive Demo with Spark").config("spark.sql.warehouse.dir", "/user/hive/warehouses/").enableHiveSupport().getOrCreate()

spark_hive.sql("show databases").show()

spark_hive.sql("Select id, title, score from default.spark_scala_posts order by score desc").show()

// *****************************************************************************************************
// *** Aggregating, Grouping and Joining ***

val commentsDF = spark.read.parquet("/user/cloudera/stackexchange/comments_parquet")
commentsDF.show(5)

// Group by and then aggregate
// Get grouped data
commentsDF.groupBy("UserId")
commentsDF.groupBy("UserId").getClass

commentsDF.groupBy("UserId").sum("Score").show(5)
commentsDF.groupBy("UserId").sum("Score").sort($"sum(Score)".desc).show(5)

// Which one has the greater average
commentsDF.groupBy("UserId").avg("Score").sort($"avg(Score)".desc).show(5)

// Use agg
commentsDF.groupBy("UserId").agg(avg("Score"), count("Score").alias("Total")).show(5)
commentsDF.groupBy("UserId").agg(avg("Score"), count("Score").alias("Total")).sort($"avg(Score)".desc).show(5)
commentsDF.groupBy("UserId").agg(avg("Score"), count("Score").alias("Total")).sort($"Total".desc).show(5)

// Describe
commentsDF.describe().show(5)
commentsDF.describe("Score").show(5)

// Joining Data
postsDF
val ask_questionsDF = postsDF.where("PostTypeId = 1").select("OwnerUserId").distinct()
val answer_questionsDF = postsDF.where("PostTypeId = 2").select("OwnerUserId").distinct()

postsDF.select("OwnerUserId").distinct().count()
ask_questionsDF.count()
answer_questionsDF.count()

// Join, but gives you a warning
ask_questionsDF.join(answer_questionsDF, ask_questionsDF("OwnerUserId") === answer_questionsDF("OwnerUserId")).count()

// Use aliases
val ask = ask_questionsDF.as("ask")
val answer = answer_questionsDF.as("answer")
ask.join(answer, col("ask.OwnerUserId") === col("answer.OwnerUserId")).count()

// Spark SQL
commentsDF.createOrReplaceTempView("comments")
spark.sql("Select UserId, count(Score) as TotalCount From comments Group by UserId Order by TotalCount desc").show(5)


// *****************************************************************************************************
// *** The Catalog API ***

spark.catalog
spark.catalog.listDatabases()
spark.catalog.listDatabases().show()

spark.catalog.listTables("default")
spark.catalog.listTables("default").show()

spark.catalog.listTables("default").count()
spark.catalog.dropTempView("comments")
len(spark.catalog.listTables("default").count()