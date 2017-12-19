package com.spark.deb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Ascending

object SortNumberFile {
  
    val conf = new SparkConf().setMaster("local[*]")
    val sparkContext = SparkSession.builder().config(conf).appName("SortNumFile").getOrCreate().sparkContext
    sparkContext.setLogLevel("ERROR")
    
  
  def main(args:Array[String]){

    val fileData = sparkContext.textFile("data/NumFileForSort.txt")
    
    val dd = fileData.map(_.toInt).sortBy(identity)
    //input.sortBy(_.toInt)
    
    
    val randRDD = fileData.map{x => (x.toInt,1) }
    
    val counts = randRDD.count()
    
    val rPartitioner = new org.apache.spark.RangePartitioner(3, randRDD)
    
    val rtt = randRDD.repartitionAndSortWithinPartitions(rPartitioner) 
      
    
    //def myfunc(index: Int, iter: Iterator[(Int, Int)]) : Iterator[String] = {
         //iter.toList.map(x => "[partID:" +  index+ ", val: " + x  +"]").iterator
    def myfunc(index: Int, iter: Iterator[(Int, Int)]) : Iterator[Int] = {
         iter.toList.map(x => x._1).iterator      
    }
    
   // rtt.mapPartitionsWithIndex(myfunc).collect().foreach(println)

    
    val rtt2 = randRDD.partitionBy(rPartitioner).sortByKey(true)
    
    rtt2.mapPartitionsWithIndex(myfunc).collect().foreach(println)
    }
}