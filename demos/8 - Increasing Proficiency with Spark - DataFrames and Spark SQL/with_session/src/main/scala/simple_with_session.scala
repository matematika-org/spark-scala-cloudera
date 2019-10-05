/**
 * Shows a simple Scala application being executed in with a session
 *
 * Bundle first with 
 *   sbt package
 * Submit using
 *   spark2-submit target/scala
 * 
 * Demo for Pluralsight course:
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

import org.apache.spark.sql.SparkSession

object NoSession {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("yarn")
      .appName("StackOverflow Test") 
      .config("spark.executor.memory", "1g") 
      .getOrCreate()

    println("*****************************************")
    println("*** Testing simple_with_session.scala ***")
    println("Application name: " + spark.sparkContext.appName + " / Version: " + spark.version) 
    println("*****************************************")
    
    spark.stop()
  }
}