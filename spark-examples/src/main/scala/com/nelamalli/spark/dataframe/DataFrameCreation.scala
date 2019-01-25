package com.nelamalli.spark.dataframe

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameCreation {

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._

    val header = Seq("col1","col2")
    val fields = header
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val data = Seq(("Databricks", "20000"), ("Spark", "100000"), ("Hadoop", "3000"))
    val rowData = data
      .map(attributes => Row(attributes._1, attributes._2))

    val rdd = spark.sparkContext.parallelize(data)
    //convert RDD[T] to RDD[Row]
    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))

    //From RDD (USING toDF())
    val dfFromRDD1 = rdd.toDF("col1","col2")

    //From RDD (USING createDataFrame)
    val dfFromRDD2 = spark.createDataFrame(rdd).toDF(header:_*)

    //From RDD (USING createDataFrame and Adding schema using StructType)
    val dfFromRDD3 = spark.createDataFrame(rowRDD,schema)

    //From Data (USING toDF())
    val dfFromData1 = data.toDF()

    //From Data (USING createDataFrame)
    var dfFromData2 = spark.createDataFrame(data).toDF(header:_*)

    //From Data (USING createDataFrame and Adding schema using StructType)
    import scala.collection.JavaConversions._
    //import scala.collection.JavaConverters._
    var dfFromData3 = spark.createDataFrame(rowData,schema)

    //From Data (USING createDataFrame and Adding bean class)
    //To-DO
  }
}
