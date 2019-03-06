package com.nelamalli.spark.dataframe.avro
import java.io.File
import org.apache.avro.Schema
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._


object AvroUsingDataBricks {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(Row("James ","","Smith","36636","M",3000),
      Row("Michael ","Rose","","40288","M",4000),
      Row("Robert ","","Williams","42114","M",4000),
      Row("Maria ","Anne","Jones","39192","F",4000),
      Row("Jen","Mary","Brown","","F",-1)
    )

    val schema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    /**
      * Write Avro File
      */
    df.write
      .mode(SaveMode.Overwrite)
      .avro("C:/tmp/spark_out/avro/namesAndFavColors.avro")
    //Alternatively you can specify the format to use instead:
    df.write.format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .save("C:/tmp/spark_out/avro/namesAndFavColors2.avro")

    /**
      * Read Avro File
      */
    val readDF = spark.read.avro("C:/tmp/spark_out/avro/namesAndFavColors.avro")
    //Alternatively you can specify the format to use instead:
    val readDF2 = spark.read
      .format("com.databricks.spark.avro")
      .load("C:/tmp/spark_out/avro/namesAndFavColors.avro")

    /**
      * Write Partition
      */
    df.write.partitionBy("gender")
      .mode(SaveMode.Overwrite)
      .avro("C:/tmp/spark_out/avro/namesAndFavColors_partition.avro")

    /**
      * Reading Partition Data
      */
    spark.read
      .avro("C:/tmp/spark_out/avro/namesAndFavColors.avro")
      .where(col("gender") === "M")
      .show()

    /**
      * Namespace
      */
    val name = "AvroTest"
    val namespace = "com.cloudera.spark"
    val parameters = Map("recordName" -> name, "recordNamespace" -> namespace)
    df.write.options(parameters)
      .mode(SaveMode.Overwrite)
      .avro("C:/tmp/spark_out/avro/namesAndFavColors_namespace.avro")

    /**
      * Explicit schema
      */
    //scala.io.Source.fromFile("person.avsc")
    //println(getClass.getResource("person.avsc"))
    //val schemaAvro = new Schema.Parser().parse(getClass.getResourceAsStream("person123.avsc"))
    val schemaAvro = new Schema.Parser().parse(new File("src/main/resources/person.avsc"))
    spark
      .read
      .format("com.databricks.spark.avro")
      //.schema(schema)
      .option("avroSchema", schemaAvro.toString)
      .load("C:/tmp/spark_out/avro/namesAndFavColors2.avro")
      .show()

    /**
      * Spark SQL
      */
    //CREATE TEMPORARY TABLE table_name USING com.databricks.spark.avro OPTIONS (path "input dir")
  }
}
