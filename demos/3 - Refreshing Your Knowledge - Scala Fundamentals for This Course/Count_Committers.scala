/**
 * Simple application to be used with scalac and scala
 *
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

  object Count_Committers {
    def main(args: Array[String]) {
      println("There are " + io.Source.fromFile(args(0)).getLines.size + " Spark commmitters")
    }
  }
 