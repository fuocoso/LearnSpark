package com.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafakReceiverStreamWC1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Streaming WordCount")
      .setIfMissing("spark.master","local[2]")

    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc,Seconds(5))


    //1.Define the input sources by creating input DStreams 通过定义输入源来创建input DSteram


    val zkQuorum ="linux01:2181/kafka09"
    val  topics= Map[String,Int]("test" -> 4)

   val lines: DStream[String] = KafkaUtils.createStream(
      ssc,
      zkQuorum,
      "0505-01",
      topics
    ).map(_._2)

    //2.Define the streaming computations by applying transformation and output operations to DStreams. 通过调用DStreams的转化或者输出操作来定义流式计算

    //转化操作
    val words: DStream[String] = lines.flatMap(_.split(" "))
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
