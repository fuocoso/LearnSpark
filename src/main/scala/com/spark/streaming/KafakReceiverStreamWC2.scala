package com.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafakReceiverStreamWC2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Streaming WordCount")
      .setIfMissing("spark.master","local[2]")

    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc,Seconds(5))


    //1.Define the input sources by creating input DStreams 通过定义输入源来创建input DSteram


    /**
      *   K type of Kafka message key1   消费到的数据key的类型
      *  V type of Kafka message value   消费到的数据的类型value,实际要处理的数据
      *  U type of Kafka message key decoder  对应的key的解码器 反序列
      *  T type of Kafka message value decoder  对应的的解码器 反序列
      *  return DStream of (Kafka message key, Kafka message value)value  返回了包含（key,value）的二元组，实际上要的是vlaue
      */

      // def createStream[K: ClassTag, V: ClassTag, U <: Decoder[_]: ClassTag, T <: Decoder[_]: ClassTag](
    /* * ssc: StreamingContext,              给定已存在的streamingContext
      * kafkaParams: Map[String, String],    给定消费者参数
      * topics: Map[String, Int],           给定消费的topic，以及对应的消费者的个数
      * storageLevel: StorageLevel          Receiver接收到的block在Executor中缓存级别
       *  ):
      */

    val kafkaParams = Map[String,String](
      "zookeeper.connect" -> "linux01:2181/kafka09",
      "group.id" -> "re02",
      "zookeeper.connection.timeout.ms" -> "10000",
        "auto.offset.reset"-> "smallest"
    )

      val  topics= Map[String,Int]("test" -> 4)

    val lines = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
        ssc,
        kafkaParams,
        topics,
        StorageLevel.MEMORY_AND_DISK_SER
      )

    //2.Define the streaming computations by applying transformation and output operations to DStreams. 通过调用DStreams的转化或者输出操作来定义流式计算

    //转化操作
    val words: DStream[String] = lines.map(_._2).flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map((_,1))
    val wc: DStream[(String, Int)] = pairs.reduceByKey(_+_)

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
