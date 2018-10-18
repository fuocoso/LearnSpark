package com.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object accumStreamingWC {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf()
      .setAppName("Spark Streaming WordCount")
      .setIfMissing("spark.master","local[2]")    //对于带有接收器的spark steaming应用程序而言，local[*]  * >=2,原因是有一个长运行的task，来运行接收器

    //第一种方式 初始化或者构建ssc
//    val ssc = new StreamingContext(conf,Seconds(5))

    //第二种方式 初始化或者构建ssc
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

ssc.checkpoint("data/checkpoint/accmWc")

    //1.Define the input sources by creating input DStreams 通过定义输入源来创建input DSteram
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("linux01",9999)

    //2.Define the streaming computations by applying transformation and output operations to DStreams. 通过调用DStreams的转化或者输出操作来定义流式计算

    //转化操作
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs= words.map(word =>((word),1))


    val wc= pairs.reduceByKey(_+_).updateStateByKey[Long]((seq: Seq[Int], state:Option[Long])=>{
      //seq:Seq[Long] 当前批次中每个相同key的value组成的Seq
       val currentValue = seq.sum
      //state:Option[Long] 代表当前批次之前的所有批次的累计的结果，val对于wordcount而言就是先前所有批次中相同单词出现的总次数
      val preValue = state.getOrElse(0L)

      Some(currentValue+preValue)
    })

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
