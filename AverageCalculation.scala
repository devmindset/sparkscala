package com.spark.deb

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.spark.RangePartitioner

object AverageCalculation {
  
  val conf = new SparkConf().setMaster("local[*]")
    val sparkContext = SparkSession.builder().config(conf).appName("SortNumFile").getOrCreate().sparkContext
    sparkContext.setLogLevel("ERROR")
    
  
  def main(args:Array[String]){

    val fileData = sparkContext.textFile("data/NumFileForSort.txt")
    val sum = fileData.map(_.toInt).reduce(_+_)
    val avg = sum / fileData.count()
    println(avg)
    
    val sum3 = fileData.map(_.toInt).fold(0)(_+_)
    val avg3 = sum3 / fileData.count()
    println("Using foldLeft = "+avg3)
    
    var totalSum = sparkContext.accumulator(0L, "Total Sum per Partiotion") 
    var totalCount = sparkContext.accumulator(0,"Total Number per Partiotion")
    
    val randRDD = fileData.map{x => (x.toInt,1) }
    val rPartitioner = new org.apache.spark.RangePartitioner(3, randRDD)
    val rtt = randRDD.partitionBy(rPartitioner)  
    
    rtt.foreachPartition { partition => {
         val array = partition.toArray;
         val sum = array.toList.map(f=>f._1).sum
         val size =  array.size
         totalSum+=sum.toLong
         totalCount+=size.toInt
       }
    }
    
    val avg2 = totalSum.value/totalCount.value 
    println("Using Accumulator = "+avg)
    
   
    
  }
}