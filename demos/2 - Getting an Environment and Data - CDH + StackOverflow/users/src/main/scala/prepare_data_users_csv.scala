/**
 * Loads users from XML into HDFS
 *
 * Start spark2-shell with the command below and you can test from the REPL as well
 *   spark2-shell --packages com.databricks:spark-xml_2.11:0.4.1,com.databricks:spark-csv_2.11:1.5.0
 * Or run using
 *   sbt package
 *   spark2-submit --class "PrepareUsersApp" target/scala-2.11/users-project_2.11-1.0.jar
 * 
 * Demo for Pluralsight course:
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import scala.util.matching.Regex

object PrepareUsersApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("PrepareUsersCSV").getOrCreate() 

    // Read the posts
    val xmlUsers = spark.sparkContext.newAPIHadoopFile("/user/cloudera/stackexchange/Users.xml", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Get only the node as org.apache.hadoop.io.Text, convert to string, get only row nodes and convert to scala.xml.Elem
    val eachUser = xmlUsers.map({case (x, y) => (y.toString.trim)}).filter(_.contains("<row ")).map(x => scala.xml.XML.loadString(x.toString))

    // Create a tuple with only the necessary fields
    val postFields = eachUser.map(createRecord)

    // Create a DataFrame
    val userDF = spark.createDataFrame(postFields)

    // Write as CSV to HDFS
    userDF.write.format("com.databricks.spark.csv").save("/user/cloudera/stackexchange/users_csv")
    }

// extract the values in each xml row
def createRecord(nd : scala.xml.Elem) = {
  val Id = (nd \ "@Id").toString
  val Reputation = (nd \ "@Reputation").toString
  val CreationDate = (nd \ "@CreationDate").toString
  val DisplayName = (nd \ "@DisplayName").toString
  val LastAccessDate = (nd \ "@LastAccessDate").toString 
  val Views = (nd \ "@Views").toString
  val UpVotes = (nd \ "@UpVotes").toString
  val DownVotes = (nd \ "@DownVotes").toString
  val Age = (nd \ "@Age").toString
  val AccountId = (nd \ "@AccountId").toString
  (Id, Reputation, CreationDate, DisplayName, LastAccessDate, Views, UpVotes, DownVotes, Age, AccountId)
  }
}