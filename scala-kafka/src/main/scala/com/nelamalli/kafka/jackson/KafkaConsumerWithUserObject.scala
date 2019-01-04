package com.nelamalli.kafka.jackson

import java.util.Properties

import com.nelamalli.kafka.beans.User
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object KafkaConsumerWithUserObject {

  def main(args: Array[String]): Unit = {

    val prop:Properties = new Properties()
    prop.put("group.id", "test")
    prop.put("bootstrap.servers","192.168.1.100:9092")
    prop.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer","com.nelamalli.kafka.jackson.UserDeserializer")
    prop.put("enable.auto.commit", "true")
    prop.put("auto.commit.interval.ms", "1000")

    val consumer = new KafkaConsumer[String,User](prop)

    val topics = List("topic_user")

    consumer.subscribe(topics.asJava)

    //consumer.subscribe(Collections.singletonList("topic_partition"))

    //consumer.subscribe(Pattern.compile("topic_partition"))

    while(true){

      val records = consumer.poll(10)
      for(record<-records.asScala){

        println("Topic: "+record.topic()+", Key: "+record.key() +", Value: "+record.value().getName +
          ", Offset: "+record.offset() +", Partition: "+record.partition())

      }
    }

    consumer.close()// close in finally block

  }
}
