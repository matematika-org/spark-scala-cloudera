/**
 * Loads badges from XML dump into HDFS
 *
 * Start spark2-shell with the command below and you can test from the REPL
 * spark2-shell --packages com.databricks:spark-xml_2.11:0.4.1,com.databricks:spark-csv_2.11:1.5.0
 * Or run using
 *   spark2-submit --class "PrepareBadgesCSVApp" target/scala-2.11/badges-project_2.11-1.0.jar
 * 
 * Demo for Pluralsight course:
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._

object PrepareBadgesCSVApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("PrepareBadgesCSV").getOrCreate() 

    // Read the posts
    val xmlPosts = spark.sparkContext.newAPIHadoopFile("/user/cloudera/stackexchange/Badges.xml", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Get only the node as org.apache.hadoop.io.Text, convert to string, get only row nodes and convert to scala.xml.Elem
    val eachBadge = xmlPosts.map({case (x, y) => (y.toString.trim)}).filter(_.contains("<row ")).map(x => scala.xml.XML.loadString(x.toString))

    // Create a tuple with only the necessary fields
    val badgeFields = eachBadge.map(createRecord)

    // Create a DataFrame
    val postDF = spark.createDataFrame(badgeFields)

    // Write as CSV to HDFS
    postDF.write.format("com.databricks.spark.csv").save("/user/cloudera/stackexchange/badges_csv")
    }

  def createRecord(nd : scala.xml.Elem) = {
    val badgeId =  (nd \ "@Id").toString   
    val userId = (nd \ "@UserId").toString
    val badgeName = (nd \ "@Name").toString
    val creationDate = (nd \ "@Date").toString  
    val badgeClass =  (nd \ "@Class").toString   
    val tagBased =  (nd \ "@TagBased").toString   

    (badgeId, userId, badgeName, creationDate, badgeClass, tagBased)
  }
}