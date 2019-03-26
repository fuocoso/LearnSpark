package com.learn.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer


object tN05 {
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("demo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile("C:\\Users\\admin\\Desktop\\Code\\Student\\teacher.log")//http://bigdata.edu360.cn/laozhang
    val dataRDD = data.map(line => {
      val fields = line.split("/")
      val subject = fields(2).split("[.]")(0)
      val teacher = fields(3)
      ((subject, teacher), 1)
      
    }).reduceByKey(_+_).map(t=>(t._1._1,(t._1._2,t._2)))


    
     val res3 = dataRDD.aggregateByKey(new ArrayBuffer[(String,Int)]())(
      (U:ArrayBuffer[(String,Int)],V:(String,Int))=>{
        val tmp = Array.apply(V)
        U ++= tmp	//加值用这个
        U.sortBy(_._2).takeRight(3)
      },
      (U1:ArrayBuffer[(String,Int)],U2:ArrayBuffer[(String,Int)])=>{
        U1 ++= U2	//加分区用这个
        U1.sortBy(_._2).reverse
      }
    ).flatMap(t=>{
       val subject = t._1
       t._2.map(t=>{
         (subject,t)
       })
     })
    
  //println(res3.collect().toBuffer)
    res3.collect().foreach(println)
    sc.stop()
    
  }
}