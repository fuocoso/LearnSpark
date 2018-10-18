package com.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, streaming}

object WindowStreamingWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Streaming WordCount")
      .setIfMissing("spark.master", "local[2]") //对于带有接收器的spark steaming应用程序而言，local[*]  * >=2,原因是有一个长运行的task，来运行接收器

    //第一种方式 初始化或者构建ssc
    //    val ssc = new StreamingContext(conf,Seconds(5))

    //第二种方式 初始化或者构建ssc
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.checkpoint("data/checkpoint/windowWC")


    //1.Define the input sources by creating input DStreams 通过定义输入源来创建input DSteram
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("linux01", 9999)

    //2.Define the streaming computations by applying transformation and output operations to DStreams. 通过调用DStreams的转化或者输出操作来定义流式计算

    //转化操作
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs = words.map(word => ((word), 1))

    //不带反函数，这种计算方式叫做累加式
        val wc1= pairs.reduceByKeyAndWindow(
         (a:Int,b:Int)=>a+b,
          Seconds(15),   //窗口时间
          Seconds(10)    //窗口滑动时间
        )
    // (a:Int,b:Int) => a+b, 就是相当于原来的reduceByKey，就是对相同key的vlaue的集合或者迭代器就行求和 (hello,iter(1,1,1,1,),然后再对批次之间的相同key的value值再进行累加 batch1:(hello,12) batch2:(hello,10),batch3:(hello,14)

    //   |--------------c--------------|
    //  |_____________________________|
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    // |---------d--------|_____________________________|
    //                     |--(c-d)-|
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs


    //带有反函数的这种叫做叠加式，会在两个窗口重叠部分计算非重叠部分的值
    val wc2 = pairs.reduceByKeyAndWindow(
      (a:Int,b:Int) => a+b,
      (c:Int,d:Int) => c-d,
      Seconds(15),
      Seconds(10)
    )

    //输出操作
    wc2.print()

    //3.Start receiving data and processing it using streamingContext.start() 启动接收器接受数据开始处理
    ssc.start()

    //4.Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination(). 等待处理被停止，可能是手动或者是错误引起
    ssc.awaitTermination()

    //5.The processing can be manually stopped using streamingContext.stop(). 通过调用stop来手动停止处理
    ssc.stop()


  }
}
