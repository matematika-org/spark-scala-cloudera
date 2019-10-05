/**
 * Show a simple use of RDD API using Spark's committer list
 * This is not the most efficient way. However, wanted to demonstrate a few Spark operations.
 *
 * Demo for Pluralsight course:
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

val committersRDD = sc.textFile("file:///stackexchange/spark-committers.tsv")

committersRDD.count()

val committers_companyRDD = committersRDD.map(line => line.split("\t")).filter(x => x(0) != "Name").map(x => (x(0), x(1)))

val top_companiesRDD = committers_companyRDD.map(p => (p._2, 1)).reduceByKey(_ + _)

val top_companies_sortedRDD = top_companiesRDD.map(x => x.swap).sortByKey(false).map(x => x.swap)

top_companies_sortedRDD.collect().foreach(println)