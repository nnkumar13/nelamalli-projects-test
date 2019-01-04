package com.nelamalli.spark.streaming

/*
* How to test?
* If you are running on windows, Download https://eternallybored.org/misc/netcat/
* Extract the content,
* From command mode run below command
* nc -l -p 9090
* Run this program
* Now go to command prompt and type few lines
* and you should see the ouput on spark console
*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkStreamingFromSocket {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9090")
      .load()

    val wordsDF = df.select(explode(split(df("value")," ")).alias("word"))

    val count = wordsDF.groupBy("word").count()

    val query = count.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    query.awaitTermination()

  }
}
