package com.nelamalli.spark.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameFromRDD {

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.parallelize(Seq(("Databricks", 20000), ("Spark", 100000), ("Hadoop", 3000)))

    val df = rdd.toDF()

    //TO-DO use df variable
  }
}
