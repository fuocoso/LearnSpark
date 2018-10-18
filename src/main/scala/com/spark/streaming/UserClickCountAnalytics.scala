package com.spark.streaming

import com.alibaba.fastjson.JSON
import com.spark.kafka.KafkaProperties
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils


object UserClickCountAnalytics {
  def main(args: Array[String]): Unit = {
    

    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster("local[2]").setAppName("UserClickCountStat")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set(s"${KafkaProperties.KAFKA_USER_TOPIC}")
    println(s"Topics: ${topics}.")

    val brokers = KafkaProperties.KAFKA_ADDR
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset"-> "smallest"
    )

    //val clickHashKey = "app::users::click"

    // Create a direct stream
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils
        .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.flatMap(line => {
      println(s"Line ${line}.")
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    // Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getLong("click_count"))).reduceByKey(_ + _)
    userClicks.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
