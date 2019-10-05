/**
 * Loads tags XML as JSON in HDFS
 *
 * Start spark2-shell with the command below and you can test from the REPL as well
 *   spark2-shell --packages com.databricks:spark-xml_2.11:0.4.1,com.databricks:spark-csv_2.11:1.5.0
 * Or run using
 *   sbt package
 *   spark2-submit --class "PrepareTagsJSONApp" target/scala-2.11/tags-project_2.11-1.0.jar
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

object PrepareTagsJSONApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("PrepareTagsJSON").getOrCreate() 

    // Read the tags
    val xmlTags = spark.sparkContext.newAPIHadoopFile("/user/cloudera/stackexchange/Tags.xml", classOf[TextInputFormat], classOf[LongWritable], classOf[Text])

    // Get only the node as org.apache.hadoop.io.Text, convert to string, get only row nodes and convert to scala.xml.Elem
    val eachTag = xmlTags.map({case (x, y) => (y.toString.trim)}).filter(_.contains("<row ")).map(x => scala.xml.XML.loadString(x.toString))

    // Create a tuple with only the necessary fields
    val tagsFields = eachTag.map(createRecord)

    // Add a schema
    val tags_schema = 
    StructType(
      Array(
        StructField("Id",StringType),
        StructField("TagName",StringType),
        StructField("Count",StringType),
        StructField("ExcerptPostId",StringType),
        StructField("WikiPostId",StringType)))

    // Create a DataFrame
    val tagsDF = spark.createDataFrame(tagsFields, tags_schema)

    // Write as CSV to HDFS
    tagsDF.write.json("/user/cloudera/stackexchange/tags_json")
    }

// extract the values in each xml row
def createRecord(nd : scala.xml.Elem) = {
  val tag_id = (nd \ "@Id").toString 
  val tag_name = (nd \ "@TagName").toString
  val count = (nd \ "@Count").toString 
  val excerpt_post_id = (nd \ "@ExcerptPostId").toString 
  val wiki_post_id = (nd \ "@WikiPostId").toString 
  
  Row(tag_id, tag_name, count, excerpt_post_id, wiki_post_id)
  }
}