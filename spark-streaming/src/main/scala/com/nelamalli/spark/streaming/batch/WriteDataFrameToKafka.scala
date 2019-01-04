package com.nelamalli.spark.streaming.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteDataFrameToKafka {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val filePath = "src/main/resources/kv.csv"

    val df:DataFrame = spark.read.option("header","true").csv(filePath)

    // since we are reading from a file which is already in text, selectExpr is optional.
    // If the bytes of the Kafka records represent UTF8 strings,
    // we can simply use a cast to convert the binary data into the correct type.
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers","192.168.1.100:9092")
      .option("topic","topic_text")
      .save()
  }
}
