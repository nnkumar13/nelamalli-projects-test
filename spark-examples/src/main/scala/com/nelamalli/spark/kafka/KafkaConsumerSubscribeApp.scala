package com.nelamalli.spark.kafka

import java.util.{Collections, Properties}
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object KafkaConsumerSubscribeApp {

  def main(args: Array[String]): Unit = {

    val prop:Properties = new Properties()
    prop.put("group.id", "test")
    prop.put("bootstrap.servers","192.168.1.100:9092")
    prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("enable.auto.commit", "true")
    prop.put("auto.commit.interval.ms", "1000")

    val consumer = new KafkaConsumer(prop)

    val topics = List("topic_text")

    consumer.subscribe(topics.asJava)

    //consumer.subscribe(Collections.singletonList("topic_partition"))

    //consumer.subscribe(Pattern.compile("topic_partition"))

    while(true){

      val records = consumer.poll(10)
      for(record<-records.asScala){

        println("Topic: "+record.topic()+", Key: "+record.key() +", Value: "+record.value() +
          ", Offset: "+record.offset() +", Partition: "+record.partition())

      }
    }

    consumer.close()// close in finally block

  }
}
