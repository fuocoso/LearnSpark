package com.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaDirectSteamWC2 {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf()
      .setAppName("Spark Streaming WordCount")
      .setIfMissing("spark.master","local[2]")


    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    /**
      * tparam K type of Kafka message key     消费到的message的key的数据类型
      * tparam V type of Kafka message value   消费到的message的的数据类型
      * tparam KD type of Kafka message key decoder 消费到的message的key的解码器（反序列化）
      * tparam VD type of Kafka message value decoder 消费到的message的的解码器（反序列化）
      * tparam R type returned by messageHandler    由 messageHandler的返回结果的类型和整体的返回类型是一致
      * return DStream of R  当前createDirectStream的返回类型
      */


    //1.Define the input sources by creating input DStreams 通过定义输入源来创建input DSteram
    val kafkaParams = Map[String,String](
      "bootstrap.servers" ->"linux01:9092,linux01:9093,linux01:9094,linux01:9095",
      "group.id" -> "dir02",
      "zookeeper.connection.timeout.ms" -> "10000",
      "auto.offset.reset"-> "smallest"
    )

    val topics = Set("test")

    /**
      * Direact 方式采用的kafka消费者的low lever api（simple api），可以自由指定消费者从patriton的哪个offset开始消费，以及如何更新offset和在哪儿保存offset
      */
    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaParams,
      topics
).map(_._2)

    //2.Define the streaming computations by applying transformation and output operations to DStreams. 通过调用DStreams的转化或者输出操作来定义流式计算

    //转化操作
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs = words.map(word =>((word),1))
    val a = words.map(word =>("key",1))

    val wc= pairs.reduceByKey(_+_)
    //(hello,20) (word,30)
    val b: DStream[(String, Long)] =words.countByValue()


    val total: DStream[(String, Int)] = a.reduceByKey(_+_)


    //输出操作
    wc.print()

    //3.Start receiving data and processing it using streamingContext.start() 启动接收器接受数据开始处理
    ssc.start()

    //4.Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination(). 等待处理被停止，可能是手动或者是错误引起
    ssc.awaitTermination()

    //5.The processing can be manually stopped using streamingContext.stop(). 通过调用stop来手动停止处理
    ssc.stop()



  }
}
