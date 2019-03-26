package com.spark.core


import org.apache.spark.{SparkConf, SparkContext}

object PartitionTest {
  def main(args: Array[String]): Unit = {

    val conf  = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setIfMissing("spark.master","local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val lines = sc.textFile("data/emp.txt",15)

    lines.foreachPartition(iter => {
   //   System.err.println(iter.size)
      var num = 0
      System.err.println(s"-------各个分区的数据--------")
    while (iter.hasNext){
      println(iter.next())
    }

    })

    val context = lines.glom().partitions.length

    println(context)

  }
}
