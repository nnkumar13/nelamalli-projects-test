package com.nelamalli.spark.dataframe

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameFromData {

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val data = Array((1,"Spark"),(2,"Scala"),(3,"Java"))
    val df:DataFrame = spark.createDataFrame(data)

    //Add column names to Data Frame
    val df2 = df.toDF("FirstColumn","SecondColumn")
    //Print Data Frame schema
    df2.printSchema()
    //Shows Data frame on console
    df2.show()

    //convert DataFrame to RDD[Row]
    val rdd = df2.rdd  // or df2 or df2.collect() returns same results
    for(a:Row<-rdd){
      println(a(0)+","+a(1))
    }
  }
}
