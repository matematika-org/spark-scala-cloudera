/**
 * Refreshing Your Knowledge - Scala Fundamentals for this Course
 *
 * This scala file contains the code used to refresh the knowledge required for a Spark + Scala + Cloudera course
 * Not meant to be ran as an application. The scala extension is for highlighting in an editor
 * Developing Spark Applications Using Scala and Cloudera
 * Created by Xavier Morera
 */

// ********************************************************************
// Creating Self-contained Applications, Including scalac & sbt
// First with scalac and scala
scalac Count_Committers.scala
ls
scala Count_Committers spark-committers-no-header.tsv

//Now sbt
mkdir sbttest
cd sbttest/
mkdir -p src/{main,test}/{java,resources,scala}
mkdir lib project target
find .

// Get the code ready
cp ./../Count_Committers.scala ./src/main/scala/
ls src/main/scala/
ls
vi build.sbt
/**
name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.8"
*/

// Package and run
sbt package
cd target/scala-2.11/
ls
scala simple-project_2.11-1.0.jar ./../../../spark-committers-no-header.tsv


// ********************************************************************
// The Scala Shell: REPL (Read Evaluate Print Loop)

// # scala


// ********************************************************************
// Scala - The Language

// Everything is an object
40
40.getClass
40 + 2
40.+(2)
res3 // Use your own numbering

// Variables
x = 41
var x = 41
x = x + 1
x = "forty one"
var x: Int = 41

// More on variables
var platform: String = "Spark";
var platform: String = "Spark"
platform
val fixedPlatform = "Spark"
fixedPlatform = "Apache Spark"
platform = "Apache Spark"
println(fixedplatform)

// Variables and operations
true
false
platform = "Apache Spark"
platform == "Spark"
10 * 100
10 * (100 * 1000) == (10 * 100) * 1000
10 * 100 * 1000 == 1000 * 100 * 10
10 - 100 - 1000 == 1000 - 100 – 10
:history


// ********************************************************************
// More on Types, Functions and Operations

// Chars and Strings
'c'
"string"
"string".length
"string".take(1)
"string".take(4)
val name = "Xavier"
println(s"Hello, $name")

// Multiline string
"""This is a 
"multiline" string"""
// resxx
<xml>This is an xml sample</xml>

// Generics and some collections
val myList = List[String] ("Xavier", "Irene")
val myMap = Map[String, String] ("Xavier" -> "Author", "Irene" -> "QA")
myList.getClass
Seq(1, 2, 3, 4)


// ********************************************************************
// Expressions, Functions and Methods

// Methods
def sumOfTwoValues(x: Int, y: Int): Int = {
  x + y
}

sumOfTwoValues(5, 7)

// Functions
val f1 = (x:Int) => x+1
f1.getClass
f1(2)


// ********************************************************************
// Classes, Case Classes and Traits

// Regular class
class PostC(Id: Integer, PostTypeId: String, Score: Integer, ViewCount: Integer, AnswerCount: Integer, OwnerUserId: String)

// A case class
case class Post (Id: Integer, PostTypeId: String, Score: Integer, ViewCount: Integer, AnswerCount: Integer, OwnerUserId: String)
val post: Post = Post(1, "1", 29, 34, 12, "32")
Post.Id

// ********************************************************************
// Flow Control
// There are 3 control structures that we will mainly use

// If
val x = 9
if (x < 10) "Less" else "More"
val x = 11
if (x < 10) "Less" else "More"

// For
for ( i <- 0 to 10 ) println(i)

// While
var i = 0
while (i < 15) {
   println(i)
   i = i + 1
}

// ********************************************************************
// Functional Programming

// Is available in Scala, but we will cover it in more detail in an upcoming Spark module
Seq(1, 2, 3).map(x => x+1)
Seq(1, 2, 3).filter(x => x %2 == 1)

// ********************************************************************
// Enter spark2-shell: Spark in the Scala Shell
// # spark2-shell