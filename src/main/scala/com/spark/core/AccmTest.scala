package com.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object AccmTest {
  def main(args: Array[String]): Unit = {
    val  conf = new SparkConf()
      .setAppName("AccMTest")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)
    val accm = new LogAccumulator
    sc.register(accm,"acTest")

    val data  = sc.parallelize(Array("1","2a","3","4b","5","6","7cd","8","9","e3"),2)

    val res = data.filter{
      case ele =>{
        val part = """^(\d+)"""
        val flag = ele.matches(part)
        if(!flag){
          accm.add(ele)
        }
        flag
      }
    }.map(_.toInt).reduce(_+_)

    println("sum:"+res)


      println(accm.value)

    sc.stop()

  }
}
