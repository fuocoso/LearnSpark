package com.spark.kafka

object KafkaProperties {

  val KAFKA_SERVER: String = "linux01"  //软编码
  val KAFKA_ADDR: String = KAFKA_SERVER + ":9092"
  val KAFKA_USER_TOPIC: String = "movieLog"
  val KAFKA_RECO_TOPIC: String = "test2"

}