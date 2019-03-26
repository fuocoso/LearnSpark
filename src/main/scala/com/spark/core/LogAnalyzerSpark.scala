package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object LogAnalyzerSpark {
 
   /**
    * Spark Application  Running Entry
    *
    * Driver Program
    * @param args
    */
   def main(args: Array[String]) {
//     // Create SparkConf Instance
//     val sparkConf = new SparkConf()
//         // 设置应用的名称, 4040监控页面显示
//         .setAppName("LogAnalyzerSpark Application")
//         // 设置程序运行环境, local
//         .setMaster("local[2]")

     val spark = SparkSession
       .builder()
       .appName("LogAnalyzerSpark Application")
       .master("local[2]")
       .getOrCreate()
     // Create SparkContext
     /**
      * -1,读取数据，创建RDD
      * -2,负责调度Job和监控
      */
     val sc = spark.sparkContext
 /**  ======================================================================= */
     /**
      * Step 1: input data -> RDD
      */
      val logFile = "data/access_log"

      val accessLogsRdd: RDD[ApacheAccessLog] = sc
          .textFile(logFile)   // read file from hdfs
          // filter
          .filter(ApacheAccessLog.isValidateLogLine)
          // parse
          .map(log => ApacheAccessLog.parseLogLine(log))

     import spark.implicits._
     accessLogsRdd.toDF().createTempView("log")

     import spark.sql

     sql("select count(1) from log").show

      println("Count:" + accessLogsRdd.count())

     /**
      * Step 2: process data -> RDD#transformation
      */
     /**
      * 需求一：计算从服务器返回的数据的平均值 最小值 最大值
	      The average, min, and max content size of responses returned from the server.
      */
     val contentSizeRdd: RDD[Long] = accessLogsRdd.map(log => log.contentSize)
     // cache
//     contentSizeRdd.cache()

     // compute
     val avgContentSize = contentSizeRdd.reduce(_ + _) / contentSizeRdd.count()
     val minContentSize = contentSizeRdd.min()
     val maxContentSize = contentSizeRdd.max()

     sql("select avg(contentSize)avg,max(contentSize) max, min(contentSize) min from log").show
//     contentSizeRdd.unpersist()
     // println
     println("Content Size Avg: %s, Min: %s, Max: %s".format(
      avgContentSize, minContentSize, maxContentSize
     ))

     /**
      * 需求二： 计算不同状态响应码出现的次数
	        A count of response code's returned.
      */
     val responseCodeToCount: Array[(Int, Int)] = accessLogsRdd
        .map(log => (log.responseCode, 1))   // WordCount
        .reduceByKey(_ + _)
        .take(5)

    println(
    s"""Response Code Count: ${responseCodeToCount.mkString("[",", ","]")}"""
    )

     /**
      * 需求三： 访问服务器的IP地址中，访问次数超过N(20)次的ip地址有哪些
	        All IPAddresses that have accessed this server more than N times.
      */
     val ipAddresses: Array[(String, Int)] =  accessLogsRdd
        .map(log => (log.ipAddress, 1))
        .reduceByKey(_ + _)
        .filter(tuple => tuple._2 >= 20)
        .take(10)
     println(
       s"""IP Addresses: ${ipAddresses.mkString("[",", ","]")}"""
     )


     /**
      * 需求四：求访问到达的页面次数的topN，前5多
	        The top endpoints requested by count.
      */
     val topEndpoints: Array[(String, Int)] = accessLogsRdd
        .map(log => (log.endpoint, 1))
        .reduceByKey(_ + _)
         .map(_.swap)
        .top(5)
       .map(_.swap)

     sql(
       """
         |select e.endpoint,e.num
         |from
         | (select
         |    endpoint,count(1) num
         |  from
         |    log
         |  group by endpoint
         |  ) e
         | order  by e.num desc limit 5
       """.stripMargin).show

     /**
        .map(tuple => (tuple._2, tuple._1))
        .sortByKey(false)
        .take(5)
        .map(tuple => (tuple._2, tuple._1))
     */
     println(
       s"""Top Endpoints: ${topEndpoints.mkString("[",", ","]")}"""
     )

     accessLogsRdd.unpersist()
 /**  ======================================================================= */
     // SparkContext Stop
     sc.stop()
 
   }
 
 }
