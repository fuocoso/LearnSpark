package com.spark.kafka

import java.util.{Properties, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object KafkaMessageProducer {

  private val users = ArrayBuffer[String]()
  for(i <- 1 to 10){
    val uuid = UUID.randomUUID().toString.replace("-","")
    users += uuid
  }
  println(users.length)

  private val random = new Random()

  private var index = -1

  def getUserID() : String = {
    index = index + 1
    if(index >= users.length) {
      index = 0
      users(index)
    } else {
      users(index)
    }
  }

  def click() : Double = {
    random.nextInt(10)
  }

  def main(args: Array[String]): Unit = {
    val topic = KafkaProperties.KAFKA_USER_TOPIC   //硬编码 "test" "demo"
    val brokers = KafkaProperties.KAFKA_ADDR
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props);

    while(true) {
      // prepare event data  {"uid":fdsf,"event_time":"123123121","os_type":"android","click_count":12.0}
      //bin/kafka-topics.sh --create --zookeeper linux01:2181/kafka09  --topic movieLog --partitions 1  --replication-factor 1

      val event = new JSONObject()
      event.put("uid", getUserID)
      event.put("event_time", System.currentTimeMillis.toString)
      event.put("os_type", "Android")
      event.put("click_count", click)

      // produce event message
      producer.send(new ProducerRecord[String, String](topic, event.toString))
      println("Message sent: " + event)

      Thread.sleep(500)
    }
  }
}
