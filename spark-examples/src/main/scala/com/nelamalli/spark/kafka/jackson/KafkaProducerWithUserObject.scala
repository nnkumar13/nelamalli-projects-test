package com.nelamalli.spark.kafka.jackson

import java.util.Properties

import com.nelamalli.spark.beans.User
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducerWithUserObject {

  def main(args: Array[String]): Unit = {

    val prop:Properties = new Properties()
    prop.put("bootstrap.servers","192.168.1.100:9092")
    prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer","com.nelamalli.spark.kafka.jackson.UserSerializer")
    prop.put("acks","all")

    val producer = new KafkaProducer[String, User](prop)

    for(i <- 0 to 100) {
      val user = new User("My Name - "+i,i)
      val record = new ProducerRecord[String, User]("topic_user",i.toString,user)
      val metadata = producer.send(record)

      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(), metadata.get().partition(),
        metadata.get().offset());

    }

    producer.close() // close in finally

  }
}
