/**
 * Shows how to use different storage mechanisms in Spark
 *
 * This scala file contains the code used in the storage demo
 * Not meant to be ran as an application. The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */
// In this sample I show how working with different types of storage is transparent to Spark
// I have uploaded the spark-committers.tsv and spark-committers-no-header.tsv to HDFS, local filesystem and S3
// Let's test reading from all 3

// Reading from HDFS in Spark on Yarn
sc.textFile("/user/cloudera/spark-committers-no-header.tsv").take(10)

// Reading from local filesystem in Spark on Yarn
sc.textFile("file:///stackexchange/spark-committers-no-header.tsv").take(10)

// Reading from S3 in Spark on Yarn
sc.textFile("s3a://pluralsight-spark-cloudera-scala/spark-committers-no-header.tsv").take(10)

// There are a couple of steps, you need to load the Hadoop and AWS libraries, it can be done with the steps below
//   spark2-shell --packages org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk:1.7.4
// You then need to set the access key and secret access key, there are several ways. You can set the access and secret key, it has to be done in all nodes
//   export AWS_ACCESS_KEY_ID="access-key"
//   export AWS_SECRET_ACCESS_KEY="secret-key"
// Another possibility is to use jceks 
//   hadoop credential create fs.s3a.access.key -provider jceks://hdfs/user/root/awskeyfile.jceks -value <accesskey>
//   hadoop credential create fs.s3a.secret.key -provider jceks://hdfs/user/root/awskeyfile.jceks -value <secretkey>
//   spark2-shell --conf spark.hadoop.hadoop.security.credential.provider.path=jceks://hdfs/user/root/awskeyfile.jceks 
// You could also set using configuration
//   sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", "<accesskey>")
//   sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", "<secretkey>")
// Or in core-site.xml using the Safety Valve from Cloudera Manager
// There are many ways, some more secure than others. Please check Cloudera's documentation for more information on setting access keys
// Additional note: please make sure ntp service is running and there is no clock offset issue. This may cause a 403 forbidden access error.
 
 



