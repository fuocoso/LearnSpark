package com.spark.kafka

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}

import com.alibaba.fastjson.JSONObject
import com.spark.kafka.ProducerConsumerLog.random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object ProducerConsumerLog {

  private val users = ArrayBuffer[String]()
  private val random = new Random()

  for(i <- 1 to 20){
    val uuid = UUID.randomUUID().toString.replace("-","")
    users += uuid
  }

  def getUserID() : String = {
   users(random.nextInt(users.size))
  }

  def getDate(): String ={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-random.nextInt(31))
    var yesterday=dateFormat.format(cal.getTime)
    yesterday
  }

  val category = Array("手机","白酒","牙膏","休闲鞋")

  val ProInfo: Map[String, Array[(String, Double)]] =  Map(
   "手机" -> Array(("Mi8",2499.00),("P20",3899.00),("iPhoneX",8889.00)),
    "白酒"->  Array(("江小白",20.00),("茅台",1988.00),("五粮液",520.00),("洋河蓝色经典",230.00)),
    "牙膏"->Array(("高露洁",3.50),("黑人",10.00),("中华",7.25)),
   "休闲鞋"->Array(("耐克",499.5),("李宁",200.00),("PUMA",329.0),("特步",300.0),("匹克",276.5))
  )

  def getOrder(): (String, String, Double) ={
    val cate: String = category(random.nextInt(category.length))
    val arr: Array[(String, Double)] = ProInfo.get(cate).get
    val pair  = arr(random.nextInt(arr.length))
    val product = pair._1
    val price = pair._2
    (cate,product,price)
  }

  val male = Array("male","female")
  def getMale() = {
    male(random.nextInt(male.size))
  }



  def main(args: Array[String]): Unit = {
    val topic = "order"
    val props = new Properties()
    val brokers = "linux01:9092"
    props.put("bootstrap.servers", brokers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props);
    val random = new Random()
     while(true) {
       val event = new JSONObject()
       event.put("date", getDate())
       val order = getOrder()
       event.put("brand",order._1)
       event.put("product",order._2)
       event.put("price",order._3)
       event.put("userid",getUserID())
       val age: Int = 18+random.nextInt(20)
       event.put("age",age)
       event.put("male",getMale)

    producer.send(new ProducerRecord[String,String](topic,event.toString))
       println(s"Message send: ${event.toString}")

      Thread.sleep(500)
    }
  }


}
