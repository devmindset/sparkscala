

package com.spark.deb

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * Sample data - 
 * John,iPhone Cover,9.99 
 * John,Headphones,5.49 
 * Jack,iPhone Cover,9.99 
 * Jill,Samsung Galaxy Cover,8.95 
 * Bob,iPad Cover,5.49 
 */

object ProductInfo {
   val conf = new SparkConf().setMaster("local")
   val sc = SparkSession
    .builder
    .appName("ProductInfo")
    .config(conf)
    .getOrCreate().sparkContext
    
      def main(args: Array[String]) {
        //Load the purchase history directly as on RDD
        val dataRdd =  sc.textFile("data/UserPurchaseHistory.csv", 3)
        //Split each line into an array of string, based on a comma, as a delimiter
        val data = dataRdd.map { x => x.split(",") }
        println(data.collect())
        /**
         * Find The total number of purchases 
         */
        println("Total Number of purchase = " +dataRdd.count())
        //The number of unique users who purchased 
        //----------------1st approach
        val uniqueUsers = data.map { x => (x(0),1) }.distinct().count()
        //----------------2nd approach
        val uniqueUsers2 = data.map { x => x(0)}.distinct().count()
        println("1st approach - Total Number of unique users who purchased = " +uniqueUsers)
        println("2st approach - Total Number of unique users who purchased = " +uniqueUsers2)
        println("Unique users-")
        data.map { x => x(0) }.distinct().collect().foreach(println)
        
        //----------------3rd approach
        type VistorID = String
        type VisitCount = Int
        val visitors: RDD[(VistorID, VisitCount)] = data.map { x => (x(0),1)}
        println("3rd approach - Total Number of unique users who purchased = "+ 
            visitors.distinct().mapValues(_ => 1).reduceByKey(_ + _).count())
            
       //----------------4th Approach   
        //Create Purchase case class
        case class Purchase (User:String,Product: String,Price:Double)    
        //Convert the RDD of Array[string] into the RDD of product case objects:    
        val caseObject = data.map { p => Purchase(p(0),p(1),p(2).toDouble) }
        val uniqueUser4= caseObject.map { x => x.User }.distinct().count() 
        println("4th approach - Total Number of unique users who purchased = " +uniqueUser4)
        //Another approach
        // val uniqueUsers = data.map{ case (user, product, price) => user }.distinct().count() 
        
        /*
         * TOTAL REVENUE 
         *  
         */
        val totalRevenue1 = data.map { x => x(2).toFloat }.sum()
        println("Total revenue = "+totalRevenue1)
        //----Print the RDD
         // data.take(5).foreach(indvArray => indvArray.foreach(println))
         //data.foreach(indvArray => indvArray.foreach(println))
        
         /*
          *  MOST POPULAR PRODUCT
          * 
          */
         val mostPopularProd = data.map { x => (x(1),1) }.reduceByKey(_+_).map(item=>item.swap)
         //println("Most popular product = "+  mostPopularProd  )
         //mostPopularProd.collect().take(5).foreach(println)
         //mostPopularProd.sortByKey(false).collect().take(5).foreach(println)
         mostPopularProd.sortByKey(false).take(1).foreach(println)
         
         
         //Approach 2
         val mostPopularProd2 = caseObject.map { x => (x.Product,1) }.reduceByKey(_+_).collect().sortBy(-_._2)
         println(mostPopularProd2(0)._1)
         
         
   }
}