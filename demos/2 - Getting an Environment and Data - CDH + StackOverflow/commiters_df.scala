/**
 * Show a simple use of Dataset[Row] API using Spark's committer list
 *
 * Demo for Pluralsight course:
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

val committersDF = spark.read.format("csv").option("header", true).option("delimiter", "\t").load("file:///stackexchange/spark-committers.tsv")

committersDF.createOrReplaceTempView("committersTable")

spark.sql("Select Organization, count(Organization) as Total From committersTable group by Organization order by count(Organization) desc").show()



