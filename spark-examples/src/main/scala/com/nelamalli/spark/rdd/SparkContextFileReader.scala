package com.nelamalli.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkContextFileReader {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[1]").appName("NNK").getOrCreate()

    val sc = spark.sparkContext
    val rdd:RDD[String] = sc.textFile("C:/000_Projects/opt/BigData/zipcodes.csv")

    rdd.take(10).foreach(println)

  }
}
