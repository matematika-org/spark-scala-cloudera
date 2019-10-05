/**
 * Includes sample code for the word count slide
 *
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// *** 1 - Why Spark with Scala and Cloudera  ***
// This module provides a quick introduction into why Spark is a top choice for working with Big Data. It then adds into the mix the use of Scala as a programming language to use the Spark API and then why Cloudera can make it easier to have an environment to work with

// *****************************************************************************************************
// *****************************************************************************************************
// *** But Why Apache Spark? ***
val lines = sc.textFile("/user/cloudera/sparkcourse/")
lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
//Tip 1: If you want to execute this code from spark2-shell, you need to create the directory in HDFS and add one or more files
//Tip 2: Add .collect() at the end if you want to see the result


