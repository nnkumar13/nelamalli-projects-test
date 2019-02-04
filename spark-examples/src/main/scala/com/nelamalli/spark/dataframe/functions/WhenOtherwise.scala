package com.nelamalli.spark.dataframe.functions


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object WhenOtherwise {


  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq((("James ","","Smith"),"36636","M"),
    (("Michael ","Rose",""),"40288","M"),
    (("Robert ","","Williams"),"42114",""),
    (("Maria ","Anne","Jones"),"39192","F"),
    (("Jen","Mary","Brown"),"","F")
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)


    import spark.sqlContext.implicits._

    val df = spark.createDataFrame(data,schema)

    df.printSchema()
    df.show()
  }
}
