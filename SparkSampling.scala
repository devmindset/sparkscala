package com.spark.deb

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkSampling {

  def main(args:Array[String]){
    
     val conf = new SparkConf().setMaster("local[*]")
     val spark: SparkSession = SparkSession
                .builder().appName("OrderItemAnalysis").config(conf).getOrCreate()
     val sc: SparkContext = spark.sparkContext
     sc.setLogLevel("ERROR")
     val data = sc.textFile("data/NumFileForSort.txt")
     //sc.makeRDD(a.takeSample(false, 1000, 1234)) - fixed num record say 1000 . Cannot use fraction
     //sample is fast because it just uses a random boolean generator that returns true fraction 
     //percent of the time and thus doesn't need to call count.
     val sample = data.sample(false, 0.2)
     println("hi");
     sample.collect().foreach(println)
   }
}

/*
3.6 sample(withReplacement, fraction, seed)   
Sample a fraction of the data, with or without replacement, using a given random number generator seed.
Note: Comparing to takeSample, the 2nd parameter of sample() is how much percentage of the total number should be sampled. However the actual number sampled may not be exactly the same.
Example 1: Fraction = 0.5 may not be exactly 50% of total numbers. It may change.
scala> val list = sc.parallelize(1 to 9)
list: org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[55] at parallelize at <console>:33

scala> list.sample(true, 0.5) .collect
res63: Array[Int] = Array(7, 9)

scala> list.sample(true, 0.5) .collect
res64: Array[Int] = Array(1, 1, 2, 4, 5, 6)
Example 2: "withReplacement"=true means output may have duplicate elements, else, it will not.
scala> list.sample(true, 1).collect
res70: Array[Int] = Array(4, 4, 4, 4, 8, 8, 9, 9)

scala> list.sample(false, 1).collect
res71: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
Example 3: If "seed" does not change, the result will not change.
scala> list.sample(true, 0.5, 1).collect
res73: Array[Int] = Array(5, 7, 8, 9, 9)

scala> list.sample(true, 0.5, 1).collect
res74: Array[Int] = Array(5, 7, 8, 9, 9)

*/