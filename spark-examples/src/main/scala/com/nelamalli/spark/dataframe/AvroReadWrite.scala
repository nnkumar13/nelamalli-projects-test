package com.nelamalli.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object AvroReadWrite {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    //spark.sparkContext.addJar("C:\\apps\\opt\\spark-2.4.0-bin-hadoop2.7\\spark-2.4.0-bin-hadoop2.7\\jars\\avro-1.8.2.jar")
    df.write.format("avro").save("C:/tmp/spark_out/avro/namesAndFavColors.avro")
  }
}
