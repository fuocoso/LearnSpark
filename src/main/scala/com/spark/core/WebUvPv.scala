package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WebUvPv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setIfMissing("spark.master","local[2]")    //在打包的时候记得要把这行代码去掉 代码的优先级 > 命令行 > 配置文件的
        .setAppName(getClass.getSimpleName)

    val sc = SparkContext.getOrCreate(conf)

    /*
    * 统计PV UV
    * select  date，count(url) pv, count(di1stinct(guid))  from tracklog where date = "2015082818";
     */

    //spark程序开发三步走
    //1. 读取外部数据，形成初始RDD
    val path = "data/2015082818"
    val log = sc.textFile(path)

    //2.调用RDD的tranformation算子实现业务需求
    //2.1从每一行取出对应的date url guid
    val data: RDD[(String, String, String)] = log.map{
      case line =>{
        val arr = line.split("\t")
        val date = arr(17).substring(0,10)
        val url = arr(1)
        val guid = arr(5)
        (date,url,guid)
      }
    }




    data.cache()

    //2.2 计算PV
    val pv: RDD[(String, Int)] = data.map(t => (t._1,1))
      .reduceByKey((a,b) => a+b)

    //2.3 计算UV
    val uv = data.map(t => (t._1,t._3))
      .distinct()
      .map(t=>(t._1,1))
      .reduceByKey((a,b) => a+b)

    val pvuv: RDD[(String, (Int, Int))] = pv.join(uv)

    //第3步，调用RDD的action算子，对结果就行保存或者是打印
    println("===PV的结果1==")
    pv.collect().foreach(println(_))

    val a: Array[(String, Int)] = pv.collect()

    pv.foreach(t =>{
      println("===PV的结果2===")
      println(t)
    })


    println("===UV的结果1==")
    uv.collect().foreach(println(_))
    uv.foreach(t =>{
      println("===UV的结果2===")
      println(t)
    })

    println("===PV $ UV 的结果1==")
    pvuv.collect().foreach(println(_))
    pvuv.foreach(t =>{
      println("===PV $ UV 的结果2===")
      println(t)
    })


  }
}
