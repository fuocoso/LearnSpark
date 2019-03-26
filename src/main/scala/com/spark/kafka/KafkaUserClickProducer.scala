package com.spark.demo

import java.util.{Properties, UUID}

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object KafkaUserClickProducer {
  private val phones = Array(("HUAWEI",Array("Honor10","P20","P20 Pro","Mate10","Play","nova3")),("XiaoMi",Array("Mi8","Mi8 Se","Mi7","6x","Note 4X","Note 5A","Mix 2s")),("Apple",Array("iPhone8 Plus","iPhone7","iPhoneX","iPhone6","iPhone6P","iPhone5S")),("OPPO",Array("FindX","R15","A5","R11","A3")),("VIVO",Array("X21","Z1","NEX","X20")),("MeiZu",Array("6","Pro7","5S","M15")),("OnePlus",Array("5T","6","5")),("Smartisan",Array("R1","Pro2","Nuts 3")))

  private val num = phones.length

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


  def getPhoneBrand: (String, Array[String]) ={
    phones(random.nextInt(num))
  }

  def getPhone = {
    val brandPhone = getPhoneBrand
    val brand = brandPhone._1
    val phoneArr = brandPhone._2
    val num = phoneArr.length
    val phone =phoneArr(random.nextInt(num))
    (brand,phone)
  }

  def main(args: Array[String]): Unit = {
    val topic = "click"
    val brokers = "bigdata.server1:9092"
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props);

    while(true) {
      val click = getPhone
      val log = getUserID()+","+click._1+","+click._2
      // produce event message
      producer.send(new ProducerRecord[String, String](topic, log))
      println("Message sent: " + log)

      Thread.sleep(500)
    }
  }
}
