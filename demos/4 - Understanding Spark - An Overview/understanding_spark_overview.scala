/**
 * Understanding Spark: An Overview
 *
 * In this module we will learn how Spark works, not yet getting too much into programming. Instead we will understand what happens at a high level
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// *****************************************************************************************************
// *** Spark, Word Count, Operations and Transformations
// Some SQL statements
// select count(*) from posts
// select distinct(tag) from posts

// *****************************************************************************************************
// *** A Few Words on Fine Grained Transformations and Scalability
// A sample in SQL
// Update Posts set Tags = '[apache-spark,sql]' where PostId=1

// *****************************************************************************************************
// *** Word Count in "Not Big Data"
// This is used to tease on using C#, not a useful scenario
// using System.Collections.Concurrent;
// ConcurrentDictionary<string, int> words;

// *****************************************************************************************************
// *** How Word Count Works, Featuring Coarse Grained Transformations
val lines = sc.textFile("file:///se/simple_titles.txt")
val words = lines.flatMap(line => line.split(" "))
val word_for_count = words.map(x => (x,1))
word_for_count.reduceByKey((x,y) => (x + y)).collect()

// Or in a single line
sc.textFile("file:///se/simple_titles.txt").flatMap(line => line.split(" ")).map(x => (x,1)).reduceByKey((x,y) => (x + y)).collect()

// *****************************************************************************************************
// *** Lazy Execution, Lineage, Directed Acyclic Graph (DAG) and Fault Tolerance
val lines = sc.textFile("file:///se/simple_titles.txt")
val words = lines.flatMap(line => line.split(" "))
val word_for_count = words.map(x => (x,1))
word_for_count.reduceByKey((x,y) => (x + y)).collect()