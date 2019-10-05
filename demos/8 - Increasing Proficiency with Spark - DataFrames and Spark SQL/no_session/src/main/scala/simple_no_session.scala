/**
 * Shows a simple Scala application being executed in without a session
 *
 * This file will not build because the SparkSession has to be created manually in any self contained application
 * 
 * Demo for Pluralsight course:
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

import org.apache.spark.sql.SparkSession

object NoSession {
  def main(args: Array[String]) {

  println("*****************************************")
  println ("*** Testing simple_no_session.scala ***")
  print ("Application name: " + spark.sparkContext.appName + " / Version: " + spark.version) 
  println("*****************************************")

  spark.stop()
  }
}