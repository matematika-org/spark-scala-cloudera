/**
 * Loads posts from XML into HDFS
 *
 * Start spark2-shell with the command below and you can test from the REPL as well
 *   spark2-shell --packages com.databricks:spark-xml_2.11:0.4.1,com.databricks:spark-csv_2.11:1.5.0
 * Or run using
 *   sbt package
 *   spark2-submit --class "PreparePostsHeaderCSVApp" target/scala-2.11/posts-project_2.11-1.0.jar
 * 
 * Demo for Pluralsight course:
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row;

object PreparePostsHeaderCSVApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("PreparePostsHeaderCSV").getOrCreate() 

    // Read the posts
    val xmlPosts = spark.sparkContext.newAPIHadoopFile("/user/cloudera/stackexchange/Posts.xml", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Get only the node as org.apache.hadoop.io.Text, convert to string, get only row nodes and convert to scala.xml.Elem
    val eachPost = xmlPosts.map({case (x, y) => (y.toString.trim)}).filter(_.contains("<row ")).map(x => scala.xml.XML.loadString(x.toString))

    // Create a tuple with only the necessary fields
    val postFields = eachPost.map(createRecord)

    //Create schema
    val questionsSchema = 
    StructType(
      Array(
        StructField("Id",StringType),
        StructField("PostTypeId",StringType),
        StructField("AcceptedAnswerId",StringType),
        StructField("CreationDate",StringType),
        StructField("Score",StringType),
        StructField("ViewCount",StringType),
        StructField("OwnerUserId",StringType),
        StructField("LastEditorUserId",StringType),
        StructField("LastEditDate",StringType),
        StructField("Title",StringType),
        StructField("LastActivityDate",StringType),
        StructField("Tags",StringType),
        StructField("AnswerCount",StringType),
        StructField("CommentCount",StringType),
        StructField("FavoriteCount",StringType)))

    // Create a DataFrame
    val postDF = spark.createDataFrame(postFields, questionsSchema)

    // Write as CSV to HDFS
    postDF.write.option("header","true").format("com.databricks.spark.csv").save("/user/cloudera/stackexchange/posts_all_csv_with_header")
    }

// extract the values in each xml row
def createRecord(nd : scala.xml.Elem) = {
  val postId = (nd \ "@Id").toString
  val postType = (nd \ "@PostTypeId").toString
  val acceptedanswerid = (nd \ "@AcceptedAnswerId").toString
  val creationdate = (nd \ "@CreationDate").toString
  val score = (nd \ "@Score").toString
  val viewCount = (nd \ "@ViewCount").toString
  //Body - not included
  val owneruserid = (nd \ "@OwnerUserId").toString
  val lasteditoruserid = (nd \ "@LastEditorUserId").toString
  val lasteditdate = (nd \ "@LastEditDate").toString
  val title = (nd \ "@Title").toString
  val lastactivitydate = (nd \ "@LastActivityDate").toString
  val tags = (nd \ "@Tags").toString.replace("&gt;&lt;", ",").replace("&lt;", "(").replace("&gt;", ")")
  val answercount = (nd \ "@AnswerCount").toString
  val commentcount = (nd \ "@CommentCount").toString
  val favoritecount = (nd \ "@FavoriteCount").toString
  Row(postId, postType, acceptedanswerid, creationdate, score, viewCount, owneruserid, lasteditoruserid, lasteditdate, title, lastactivitydate, tags, answercount, commentcount, favoritecount)
  }
}