package com.spark.deb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

object WordCountOptimization {
 
  val conf = new SparkConf().setMaster("local")
   val sc = SparkSession
    .builder
    .appName("ProductInfo")
    .config(conf)
    .getOrCreate().sparkContext
  
  def main(arr:Array[String]){
     val data = sc.textFile("data/wordcount")
     val flatMapData = data.flatMap { x => x.split(" ") }
     val wordPairsRDD = flatMapData.map { x => (x,1) }.partitionBy(new HashPartitioner(4))
    //Approach 1
     val count = wordPairsRDD.reduceByKey(_+_).collect();
     count.foreach(println)
    //Approach 2
     val countRdd =  wordPairsRDD.reduceByKey(_+_)
     countRdd.foreachPartition(part  => {
       val array =  part.toArray
       array.foreach(println)
     })
            
     /*check if a particular keyword exists*/
     val search ="your"
     val data2 = sc.textFile("data/wordcount",3)
     data2.foreachPartition { partition => {
               val array = partition.toArray;
               array.foreach { x =>
                 val arr = x.split(" ")
                 if(arr.find(_==search).equals(None)){
                   println("Found = "+x)
                 }
                 if(arr.contains(search)){
                   println("Found 2 = "+x)
                 }
               }
         } 
      }
     
     
     
     //FInd top N records 
     val sortRDD = wordPairsRDD.reduceByKey(_+_).map(item => item.swap)
     //sortRDD.repartitionAndSortWithinPartitions()
     
  }
}