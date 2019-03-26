package com.spark.Interview

import com.spark.Interview.Questions1.getClass
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockIp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    val names = Array("Jack", "Bob", "Allen", "Tim", "John", "Lisa", "Rose", "Adam", "Zora", "Alex", "Cannon")
    val namesSize = names.length

    val ip = Array("192.168.7.2","192.168.7.3","172.21.100.4","172.21.100.5","187.22.89.4","187.22.89.12","176.39.21.55","176.39.21.56")
    val ipSize = ip.length

    val ipA = new ArrayBuffer[(String,String)]()
    val ipB = new ArrayBuffer[(String,String)]()

    val random = new Random()
    for(i <- 0 to 100){
      if (i<70){
        ipA += ((ip(random.nextInt(ipSize)),names(random.nextInt(namesSize))))
      }

      if (i>20){
        ipB += ((ip(random.nextInt(ipSize-3)),names(random.nextInt(namesSize))))
      }
    }

    val a = ipA.toSeq
    val b = ipB.toSeq

    val rdda = spark.sparkContext.parallelize(a)
    val rddb = spark.sparkContext.parallelize(b)

    rdda.map(t => t._1+","+t._2).repartition(1).saveAsTextFile("data/ipa")
    rddb.map(t => t._1+","+t._2).repartition(1).saveAsTextFile("data/ipb")
  }
}
