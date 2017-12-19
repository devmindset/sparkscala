package com.spark.deb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object WebLogAnalysis {
   val conf = new SparkConf().setMaster("local")
   val sc = SparkSession
    .builder
    .appName("WebLogAnalysis")
    .config(conf)
    .getOrCreate().sparkContext
   
    def main(args:Array[String]){
     val data = sc.textFile("data/logfile.txt")
     var logs = data.map { line => line.trim().toLowerCase() }
     logs.persist();
     println("Total Request="+logs.count())
     
     var errorLogs = logs.filter { x => (x.split(" ")(8) == "404")}
     
     errorLogs.foreach (println)
     
     //Longest log line
     var lengths = logs.map { x => (x.length(),x ) }.collect().sortBy(-_._1).take(1)
     println("Longest log line = "+lengths(0)._2+" - Length = "+lengths(0)._1)
     
     println(logs.map { x => x.length() }.reduce(math.max))
     
     //Find out who generate most request
     var mostReq= logs.map { x => x.split(" ")(0) }
     .map { x => (x,1) }.reduceByKey(_+_).collect().sortBy(-_._2).take(1) 
     println("Most request generatd from = "+ mostReq(0)._1 )
     ///////////////////////////////////////////////////////////////
   case class Student(name: String, score: Int)  
     val alex = Student("Alex", 83)
  val david = Student("David", 80)
  val frank = Student("Frank", 85)
  val julia = Student("Julia", 90)
  val kim = Student("Kim", 95)
  
  val students = Seq(alex, david, frank, julia, kim)
  
  def max(s1: Student, s2: Student): Student = if (s1.score > s2.score) s1 else s2

  val topStudent = students.reduceLeft(max)
  println(s"${topStudent.name} had the highest score: ${topStudent.score}")

  val a = Array(20, 12, 6, 15, 2, 9)
  println(a.reduceLeft(_ min _))
  
/*  
 *  If error format is different
 *  
 *  // base RDD
val lines = sc.textFile("hdfs://...")
// transformed RDDs
val errors = lines.filter(_.startsWith("ERROR"))
val messages = errors.map(_.split("\t")).map(r => r(1))
messages.cache()
// action 1
messages.filter(_.contains("mysql")).count()
// action 2
messages.filter(_.contains("php")).count()
 */
  
 /* 
  * NOT WORKING
  * 
  * def mapRawLine(line: String): Option[Student] = {
    try {
      val fields = line.split(",", -1).map(_.trim)
      Some(
        Student(
          name = fields(0)
          ,score = fields(1).trim().toInt //.substring(13, 15)
          //,action = if (fields(2).length > 2) Some(fields(2)) else None
        )
      )
    }
    catch {
      case e: Exception =>
        println(s"Unable to parse line: $line")
        None
    }
  }
     val studentData = sc.textFile("data/student")
        
       val topStudent2 = studentData.map { x => x }.reduceLeft(max)
  println(s"${topStudent.name} had the highest score: ${topStudent.score}")
      
  */
    }
   
   ///////////////////////////////////////////////////////////////////////////
   
  //Option(x) is basically just saying if (x != null) Some(x) else None
   //Some(x) on not-null input, and None on null input.
   
   
   /*
    
    def toInt(in: String): Option[Int] = {
    try {
        Some(Integer.parseInt(in.trim))
    } catch {
        case e: NumberFormatException => None
    }
    }
    toInt(someString) match {
    case Some(i) => println(i)
    case None => println("That didn't work.")
    }
    
    */
   
}