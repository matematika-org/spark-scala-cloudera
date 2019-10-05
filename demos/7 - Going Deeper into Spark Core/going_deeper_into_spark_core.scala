/**
 * Going Deeper into Spark Core
 *
 * This module goes deeper into the RDD API
 * This file contains the code used in this module, but it is not meant to be ran as an application. 
 * The scala extension is for highlighting in an editor
 * Included in Pluralsight course:
 *  Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// ************************************************************************************************
// *** Functional Programming: Anonymous Functions (Lambda) in Spark ***
// Named functions
def split_the_line(x: String): Array[String] = x.split(",")

badges.map(split_the_line)

// Anonymous Functions
badges.map(x => x.split(","))


// ************************************************************************************************
// *** A Quick Look at Map, FlatMap, Filter and Sort ***
// Get our data, namely our words
val lines = sc.textFile("/user/cloudera/stackexchange/simple_titles.txt")
lines.collect()
for (l <- lines.collect()) println(l)

val words_in_line = lines.map(x => x.split(" "))
for (l <- words_in_line.collect()) println(l)
words_in_line.collect()

// Flatmap is to 0, 1 or more
val words = lines.flatMap(line => line.split(" "))
words.collect()

// And we apply a function over all words with map
val word_for_count = words.map(x => (x,1))
word_for_count.take(1)
word_for_count.take(5)

// Filter is another transformation where we pass the function that if returns false, the element is not moved to the next RDD
def starts_h(word: (String, Int)) = word._1.toLowerCase.startsWith("h")

word_for_count.filter(starts_h).collect()

// Let's do an aggregation - we will explain this in more detail later
word_for_count.take(1)
val word_count = word_for_count.reduceByKey(_ + _)
word_count.take(10)

word_count.sortByKey().collect()
word_count.sortByKey(false).collect()

word_count.map({ case (x,y) => (y,x) }).sortByKey(false).map(x => x.swap).collect()

word_count.sortBy({ case (x,y) => -y }).collect()

 
// ************************************************************************************************
// *** How Can I Tell It Is a Transformation *** 
val lines = sc.textFile("/user/cloudera/stackexchange/simple_titles.txt")
val words = lines.flatMap(line => line.split(" "))
val word_for_count = words.map(x => (x,1))
val grouped_words = word_for_count.reduceByKey(_ + _)
grouped_words.collect()


// ************************************************************************************************
// *** Why Do We Need Actions? ***
val lines = sc.textFile("/user/cloudera/stackexchange/simple_titles.txt")
val words = lines.flatMap(line => line.split(" "))
val word_for_count = words.map(x => (x,1))
val grouped_words = word_for_count.reduceByKey(_ + _)
grouped_words.collect()
grouped_words.saveAsTextFile("/user/cloudera/stackexchange/words")


// ************************************************************************************************
// *** Partition Operations: MapPartitions and PartitionBy ***
// Let's see which partitioner we have
badges_columns_rdd.take(1)
val badges_for_part = badges_columns_rdd.map(x => (x(2), x.mkString(","))).repartition(50)
badges_for_part.partitioner

// Now let's use a HashPartitioner 
import org.apache.spark.HashPartitioner
val badges_by_badge = badges_for_part.partitionBy(new HashPartitioner(50))
badges_by_badge.partitioner

badges_for_part.saveAsTextFile("/user/cloudera/stackexchange/badges_no_partitioner")
badges_by_badge.saveAsTextFile("/user/cloudera/stackexchange/badges_yes_partitioner")

// Use glom to see how using a partitioner worked
badges_by_badge.map({ case (x,y) => x }).glom().take(1)

// How many items per partition, with HashPartitioner
badges_by_badge.mapPartitions(x => Array(x.size).iterator, true).collect() 
// Without HashPArtitioner
badges_for_part.mapPartitions(x => Array(x.size).iterator, true).collect()

val counted_badges = badges_by_badge.mapPartitions(x => Array(x.size).iterator, true)

// ************************************************************************************************
// *** Sampling Your Data and Distinct***
posts_all.count()
// Get a sample
val sample_posts = posts_all.sample(false,0.1,50)
sample_posts.count()

// Count appoximate
posts_all.count()
posts_all.countApprox(100, 0.95)

// Take a sample
posts_all.takeSample(false,15,50)
posts_all.takeSample(false,15,50).size


// ************************************************************************************************
// *** Set Operations: join, union, ... ***
// The data for this sample, we create two RDDs, questions asked and questions responded
val questions = sc.parallelize(Array(("xavier",1),("troy",2),("xavier",5)))
val answers = sc.parallelize(Array(("xavier",3),("beth",4)))
questions.collect() 
answers.collect()

// We can use union to concatenate both RDDs
questions.union(answers).collect()
questions.union(questions).collect()
// Does not work since we have Array(String, Int)
questions.union(sc.parallelize(Array("irene", "juli", "luci"))).collect()

// And use join to determine who has asked and responded
questions.join(answers).collect()

// Or all, but with a None for those that have only either asked or responded
questions.fullOuterJoin(answers).collect()

// We can also do a left outer join to get a list of those who have only asked. If they haven't responded there will be a null
questions.leftOuterJoin(answers).collect()

// Right outer join 
questions.rightOuterJoin(answers).collect()

// Needless to say, a leftOuterJoin on one RDD is the same as a rightOuterJoin on the other one 
questions.leftOuterJoin(answers).collect()
answers.rightOuterJoin(questions).collect()

// Cartesian
questions.cartesian(answers).collect()


// ************************************************************************************************
// *** Combining, Aggregating, Reducing and Grouping on PairRDDs ***
posts_all.take(1)
val each_post_owner = posts_all.map(x => x.split(",")(6))
val posts_owner_pair_rdd = each_post_owner.map(x => (x,1))
posts_owner_pair_rdd.take(1)

// Group by Key
val top_posters_gbk = posts_owner_pair_rdd.groupByKey()
top_posters_gbk.take(10)
// List, should be all 1's
top_posters_gbk.map({ case (x,y) => (x, y.toList) }).take(10)
// How many posts per user
top_posters_gbk.map({ case (x,y) => (x, y.size) }).take(10)
// Get the top poster
top_posters_gbk.map({ case (x,y) => (x, y.size) }).sortBy({ case (x, y) => -y}).take(1)

// Reduce by key
val top_posters_rbk = posts_owner_pair_rdd.reduceByKey(_ + _)
top_posters_rbk.lookup("51")

// Both counts match
top_posters_gbk.count()
top_posters_rbk.count()

// Aggregate by key is similar to reduceByKey
val posts_all_entries = posts_all.map(x => x.split(","))
val questions = posts_all_entries.filter(x => x(1) == "1")
val user_question_score = questions.map(x => (x(6),x(4).toInt))
user_question_score.take(5).foreach(println)

// aggregateByKey
var for_keeping_count = (0,0)
def combining (tuple_sum_count: (Int, Int), next_score: Int) = (tuple_sum_count._1 + next_score, tuple_sum_count._2 + 1)
def merging (tuple_sum_count: (Int, Int), tuple_next_partition_sum_count: (Int, Int)) = (tuple_sum_count._1 + tuple_next_partition_sum_count._1, tuple_sum_count._2 + tuple_next_partition_sum_count._2)

val aggregated_user_question = user_question_score.aggregateByKey(for_keeping_count)(combining, merging)
aggregated_user_question.take(1)
aggregated_user_question.lookup("51")

// Combine by key


def to_list(postid: Int): List[Int] = List(postid)

def merge_posts(posta: List[Int], postb: Int) = postb :: posta

def combine_posts(posta: List[Int], postb: List[Int]): List[Int] = posta ++ postb

val user_post = questions.map(x => (x(6), x(0).toInt))

val combined = user_post.combineByKey(to_list, merge_posts, combine_posts)

combined.filter({ case (x,y) => x == "51" }).collect()

// Count by key
user_post.lookup("51")
user_post.countByKey()("51")


// ************************************************************************************************
// *** ReduceByKey vs. GroupByKey: Which One is Better? ***
// Both can be used for the same purpose, but they work very different internally
val reduced = word_for_count.reduceByKey(_ + _)
val grouped = word_for_count.groupByKey()

reduced.take(1)
grouped.take(1)

reduced.count()
grouped.count()


// ************************************************************************************************
// *** Grouping Data into Buckets with Histogram ***
badges_reduced.take(10)
badges_reduced.map({ case (x,y) => y }).histogram(7)
val intervals: Array[Double] = Array(0,1000,2000,3000,4000,5000,6000,7000)
badges_reduced.map({ case (x,y) => y }).histogram(intervals)
badges_reduced.sortBy(x => -x._2).take(10)
badges_reduced.filter(x => x._2 < 1000).count()


// ************************************************************************************************
// *** Caching and Data Persistence ***
badges_reduced.setName("Reduced RDD")
badges_reduced.cache()

badges_reduced.cache()
import org.apache.spark.storage.StorageLevel
badges_count_badge.persist(StorageLevel.DISK_ONLY)


// ************************************************************************************************
// *** Shared Variables: Accumulators and Broadcast ***
val counted_badges = badges_by_badge.mapPartitions(x => Array(x.size).iterator, true)
counted_badges.collect()
counted_badges.collect().sum
counted_badges.toDebugString

// Create accumulator
val accumulator_badge = sc.longAccumulator("Badge Accumulator")

// Create a function that will increase the value
def add_badge(item: (String, String)) = accumulator_badge.add(1)

// Execute with every item, this is an action
badges_by_badge.foreach(add_badge)

// Get final value
accumulator_badge.value
 
// Broadcast variable
val users_all = sc.textFile("/user/cloudera/stackexchange/users_csv")
users_all.take(10)
val users_columns = users_all.map(split_the_line)
users_columns.take(3)

top_posters_rbk.take(10)
top_posters_rbk.lookup("51")

val tp = top_posters_rbk.collectAsMap()
val broadcast_tp = sc.broadcast(tp)

def get_name(user_column: Array[String]) = {
    val user_id = user_column(0)
    val user_name = user_column(3)    
    var user_post_count = "0"
    if (broadcast_tp.value.keySet.exists(_ == user_id))
        user_post_count = broadcast_tp.value(user_id).toString
    (user_id, user_name, user_post_count)
    }

val user_info = users_columns.map(get_name)
user_info.take(10)


// ************************************************************************************************
// *** What's Needed for Developing Self Contained Spark Applications ***
import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
.setMaster("yarn") 
.setAppName("Self Contained Application")

val sc = new SparkContext(conf)