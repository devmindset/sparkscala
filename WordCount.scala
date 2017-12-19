package com.spark.deb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import scala.io.Source

object WordCount {
  
   val conf = new SparkConf().setMaster("local")
   val sc = SparkSession
    .builder
    .appName("ProductInfo")
    .config(conf)
    .getOrCreate().sparkContext
    
     def loadCommonWords(): List[String] = Source.fromInputStream(getClass.getResourceAsStream("data/commonwords"))
       .getLines()
       .filter(!_.isEmpty())
       .filter(!_.startsWith("#"))
       .toList
       
     case class TextStats(totalChars: Long, totalWords: Long, mostFrequentWords: Seq[(String, Int)]) {
          override def toString = s"characters: $totalChars, " +
           s"words: $totalWords, " +
           "the most frequent words:\n" + mostFrequentWords.mkString("\n")
     }  
    
    def main(args:Array[String]){
       val data = sc.textFile("data/wordcount")
       
       val flatMapData = data.flatMap { x => x.split(" ") }
       
       val wordCount = flatMapData.map { x => (x,1) }.reduceByKey(_+_)
       
       println(wordCount.collect().mkString("-"))
       //Output - (are,2)-(you,1)-(Hello,1)-(how,1)
       
       /*Print - Number of word and duplicate word per line*/
       var acc = sc.accumulator(0, "Total Words")  
       
       
       var lineData = data.map { x => (x.split(" ").length)}
       lineData.zipWithIndex().foreach(f=> println("Line# "+f._2+" - "+f._1))
       
       val lineData2 = data.zipWithIndex().map { case (line, i) => line.split(" ").length + ", " + i }
       lineData2.foreach (println)
       
       /*Print - Duplicate word per line */
       data.zipWithIndex().foreach{f=> 
         val arrLine = f._1.toString().split(" ").toList
         //println(arrLine)
         val duplicateList = arrLine.diff(arrLine.distinct).distinct
         println("NumOfWords ="+arrLine.size + " Duplicate word = "+duplicateList +" - Duplicate word count = "+ duplicateList.size +" - Line#"+ f._2)
         
       }
       
       /*Print - Number of character*/
       var charCount = flatMapData.map { x => (1,x.length()) }.reduceByKey(_+_)
       charCount.collect().foreach(println) 
       
       val app2CharCount = flatMapData.map { x => (x.length()) }
       println("CharacterCount = "+app2CharCount.sum().toInt)
       
    
       /*Print the lines containing a specific word*/
       var search = "how"
       //You can use contains also
       val containsData = flatMapData.filter( x => x.equals("are") ).count()
       println("Specific word count = "+containsData)
       /*Print the lines containing specific word */
       data.filter( x => x.contains("are") ).foreach(println)
       println("Number of lines containing specific word = "+data.filter( x => x.contains("are") ).count())
       
 /* Read from spark console */
//      val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
//      val sc = new SparkContext(conf)
//      val distFile = sc.textFile("bible.txt")
//      print("Enter word to loook for in the HOLY BILE: ")
//      val word = Console.readLine

       
       /**
        * USING ACCUMULATOR 
        */
       
       var words = data.flatMap { x => x.split(" ") }
       words.foreach { x => acc += x.length() }
       println("Total_Num_Of_Char = "+acc.value)
       
       
       var totalWords = sc.accumulator(0L, "Total Words")
       var totalChars = sc.accumulator(0, "Total Chars")
       flatMapData.foreach { x => totalWords+=1 }
       println("WordCount using accumulator = "+totalWords.value)
       
       flatMapData.foreach (totalChars +=_.length())
       println("CharCount using accumulator = "+totalChars.value)
       
       /**
        * USING BROADCAST VARIABLE
        * 
        * Calculate most frequently used words
        */
       
       
       //Source.fromInputStream(getClass.getResourceAsStream("data/commonwords")).getLines()
       
       val cc = sc.textFile("data/commonwords").flatMap { x => x.split(" ") }.filter( x => !x.startsWith("#")).filter { x => !x.isEmpty() }
       val common = cc.collect().toList
       println(common.toString()) 
       
//       val list: List[(String, String)] = rdd.collect().toList
//       val col1: List[String] = list.map(_._1)
//       val col2: List[String] = list.map(_._2)
       
       val _commonWords = sc.broadcast(common)
       
       println("Commonwords using broadcast variable = "+ _commonWords.value)
       
       //Print most frequently used words 
       
       val wordCounts = flatMapData
      .filter(!_commonWords.value.contains(_))  // Filter out all too common words
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

      wordCounts.collect().foreach(println)
      
      println(TextStats(totalChars.value, flatMapData.count(), wordCounts.take(2)))
      
      
       
     }
  
}