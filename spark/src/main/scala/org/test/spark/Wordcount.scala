package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Wordcount {
  
  def main( args:Array[String]) = {
    val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
    
    val sc = new SparkContext( conf )    
    
    val testRdd = sc.textFile("food.txt")
    
    testRdd.flatMap { line =>
      line.split(" ")      
    }.map { word => 
      (word,1)
      }
    .reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")  
  }
}