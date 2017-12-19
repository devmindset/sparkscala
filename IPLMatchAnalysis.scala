package com.spark.deb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
/**
 * CSV Data Format - 
 * id,season,city,date,team1,team2,toss_winner,toss_decision,result,dl_applied,winner,win_by_runs,win_by_wickets,player_of_match,venue,umpire1,umpire2,umpire3
 * 
 * 
 */
object IPLMatchAnalysis {
  def main(args:Array[String]){
    
   val conf = new SparkConf().setMaster("local")
   val sc = SparkSession
    .builder
    .appName("ProductInfo")
    .config(conf)
    .getOrCreate().sparkContext
   
     //Load the purchase history directly as on RDD
     val dataRdd =  sc.textFile("data/matches.csv", 3)
     //Split each line into an array of string, based on a comma, as a delimiter
     val data = dataRdd.map { x => x.split(",") }
    
    /**
     * Which stadium is best suitable for first batting 
     * ------------------------------------------------
     * win_by_runs means – Team batted first and won the Match by margin of some runs.
     * win_by_wickets means – Team batted second and chased the target successfully.
     * So we will take out the columns toss_decision, won_by_runs, won_by_wickets, venue. 
     * From this we will filter out the columns which are having won_by_runs value as 0 so that 
     * we can get the teams which won by batting first. Here is the scala code to do that.
     */
     
    val fil = data.map { x => (x(7),x(11),x(12),x(14)) }
    //Find  won_by_runs value > 0
    val wonByRuns = fil.filter(f => if(f._2.toInt != 0) true else false)
    //wonByRuns.collect().foreach(println)
    //wonByRuns.map(f => (f._4 ,1)).collect().foreach(println)
    
    val bat_first_won_per_venue = wonByRuns.map(f => (f._4 ,1)).reduceByKey(_+_).map(item=>item.swap).sortByKey(false)
    println("Highest Bat First winning count = "+bat_first_won_per_venue.take(1)(0)._2)
    
    val bat_first_won_per_venue_unSorted = wonByRuns.map(f => (f._4 ,1)).reduceByKey(_+_)
    
    val batFirstWonCountPerVenue = wonByRuns.map(f => (f._4 ,1)).reduceByKey(_+_).collect().sortBy(-_._2)
    batFirstWonCountPerVenue.foreach(println)
    
    //Now calculate how many matches that each stadium has been venued
    
    val matchCountPerVenue = fil.map(f => (f._4,1)).reduceByKey(_+_).collect().sortBy(-_._2)
    matchCountPerVenue.foreach(println)
    //Alternate syntax map(_.swap)
     val match_Count_Per_Venue = fil.map(f => (f._4,1)).reduceByKey(_+_).map(item=>item.swap).sortByKey(false)
     
      val match_Count_Per_Venue_unSorted = fil.map(f => (f._4,1)).reduceByKey(_+_)
    
    //Now calculate winning percentage
    
   //We cannot join array it needs to be converted into RDD - val fCRDD = sc.parallelize(batFirstWonCountPerVenue) the only we can join
    
    
    //bat_first_won_per_venue.join(match_Count_Per_Venue).map(x=>(x._1._1,((x._2._1.toInt*100)/x._2._1.toInt))).collect.foreach(println)
    
    val join = bat_first_won_per_venue_unSorted.join(match_Count_Per_Venue_unSorted)
    
    join.map( f => ((f._1),( (f._2._1*100)/f._2._2 ) ) ).map(item=>item.swap).sortByKey(false).foreach(println)
  }
}