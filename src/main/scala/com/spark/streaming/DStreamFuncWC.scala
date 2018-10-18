package com.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DStreamFuncWC {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf()
      .setAppName("Spark Streaming WordCount")
      .setIfMissing("spark.master","local[2]")    //对于带有接收器的spark steaming应用程序而言，local[*]  * >=2,原因是有一个长运行的task，来运行接收器

    //第一种方式 初始化或者构建ssc
//    val ssc = new StreamingContext(conf,Seconds(5))

    //第二种方式 初始化或者构建ssc
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc,Seconds(5))



    //1.Define the input sources by creating input DStreams 通过定义输入源来创建input DSteram
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("linux01",9999)

    //2.Define the streaming computations by applying transformation and output operations to DStreams. 通过调用DStreams的转化或者输出操作来定义流式计算

    //转化操作
    val words: DStream[String] = lines.flatMap(_.split(" "))
   val nonEmpty = words.filter(_.nonEmpty)
    val pairs = nonEmpty.map(word =>((word),1))
    val a = words.map(word =>("key",1))

    val wc1= pairs.reduceByKey(_+_)

    //transform 和foreachRDD,可以直接基于DStream底层的RDD来直接操作，也就是可以直接使用在RDD中暴露而没有在DStream中暴露的api，二者区别如果当前的结果需要在后面处理中使用的话，就用transform，反之，后续不再使用，而是希望直接将结果进行保存那就用foreachRDD
    val wcProp1 = wc1.transform(rdd =>{
     val num =  rdd.map(_._2).sum()
      println(num)
      rdd.map(t =>{
        val word = t._1
        val count = t._2
        if(num !=0){
          (word,count/num)
        }else{
          (word,0.0)
        }
      })
    })


    val wcProp = wc1.foreachRDD(rdd =>{
      val num =  rdd.map(_._2).sum()
      println(num)
      val res: RDD[(String, Double)] =rdd.map(t =>{
        val word = t._1
        val count = t._2
        if(num !=0){
          (word,count/num)
        }else{
          (word,0.0)
        }
      })
      res.foreach(println)
    })
    //计算单词的总个数
    val num: DStream[Long] = nonEmpty.count()

    //采用countByNalue来实现wordcount
    val wc2 = nonEmpty.countByValue()

//   wcProp1.print()

    //输出操作
   // println("==总共的单词个数==")
  //  num.print()

    //println("==使用reduceByKey来计算每个批次单词出现的次数==")
    wc1.print()

   /// println("==采用countByNalue来计算每个批次单词出现的次数==")
   // wc2.print()
    wc2.saveAsTextFiles(prefix = "data/wordcount/res",suffix = s"${System.currentTimeMillis()}")



    //3.Start receiving data and processing it using streamingContext.start() 启动接收器接受数据开始处理
    ssc.start()

    //4.Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination(). 等待处理被停止，可能是手动或者是错误引起
    ssc.awaitTermination()

    //5.The processing can be manually stopped using streamingContext.stop(). 通过调用stop来手动停止处理
    ssc.stop()



  }
}
