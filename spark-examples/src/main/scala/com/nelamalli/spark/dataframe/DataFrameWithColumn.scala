package com.nelamalli.spark.dataframe

import org.apache.spark.sql.SparkSession

object DataFrameWithColumn {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val filePath="C://000_Projects/opt/BigData/zipcodes.csv"
    val dfText = spark.read.text(filePath)
    dfText.show()

    //reading as a CSV will separate the columns
    val dfcsv = spark.read.csv(filePath)
    dfcsv.show()

    //Adding an option
    spark.read.option("inferSchema","true").csv(filePath)

    //Chaining multiple options
    val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath)
    df2.show(false)
    df2.printSchema()

    //Change the column data type
    df2.withColumn("RecordNumber",df2("RecordNumber").cast("Integer"))
    df2.select(df2("RecordNumber")).printSchema()

    //Derive a new column from existing
    val df4=df2.withColumn("CopiedColumn",df2("RecordNumber")* -1)
    df4.show(2)

    //Transforming existing column
    val df5 = df2.withColumn("RecordNumber",df2("RecordNumber")*100)
    df5.show(2)

    //You can also chain withColumn to change multiple columns

    //Renaming a column. remember it returns a new RDD
    val df3=df2.withColumnRenamed("RecordNumber","SeqNo")
    df3.printSchema()

    //Droping a column
    val df6=df4.drop("CopiedColumn")
    println(df6.columns.contains("CopiedColumn"))


  }
}
