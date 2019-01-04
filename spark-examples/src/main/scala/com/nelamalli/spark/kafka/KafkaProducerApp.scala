package com.nelamalli.spark.kafka
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
object KafkaProducerApp extends App {
  //def main(args: Array[String]): Unit = {
    val topic = "topic_text"
    val prop:Properties = new Properties()
    prop.put("bootstrap.servers","192.168.1.100:9092")
    prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("acks","all")
    val producer = new KafkaProducer[String, String](prop)
    try {
      for (i <- 0 to 15) {
        val record = new ProducerRecord[String, String](topic, i.toString, "My Site is nelamalli.com " + i)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(), metadata.get().partition(),
          metadata.get().offset())
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }
  //}
}
