package com.spark.core

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object GroupSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setIfMissing("spark.master","local[2]")
      .setAppName(getClass.getSimpleName)

    val sc = SparkContext.getOrCreate(conf)

    //1.创建RDD
    val path = "data/SondSort.txt"
    val data = sc.textFile(path)

    /**
      * 第一种方式:思路简单清晰，代码量不大
      * 缺点：
      *   使用了shuffle，比较耗性能
      *   假设grooupByKey的处理的key分布不均匀，还有可能产生数据倾斜
      */
    val pair = data.map(line => {
      val arr = line.split("\t")
      val name = arr(2)
      val money =if(arr(3).nonEmpty) arr(3).toFloat else 0.0f
      (name,money)
    })

    pair.cache()

    val res1 =  pair.groupByKey()  //(allen,CompactBuffer(129.2, 49.75, 218.88, 321.2, 65.3, 87.9))
      .map(t =>{
      val name = t._1
      val top2 = t._2.toList.sorted.takeRight(2).reverse
      (name,top2)
    })
    println("=======第一种方式========")
    res1.collect().foreach(println)

    /**
      * 第2种方式; 两阶段聚合
      *   第一阶段：给原来的key打上随机散列（一般就是随机数）,经过groupByKey这样shuffle操作，将key尽量打散，进入不同的reduce，然后进行第一次聚合（sum topn），合并大量的数据
      *   第二阶段：将第一阶段聚合之后的结果的key前面的随机散列去掉，然后按照原来的key，再经过groupByKey这样shuffle操作，做相同聚合操作，得到最终的结果
      * 优点：解决了第一种方式中可能出现的数据倾斜
      * 缺点：前后经过了两次shuffle
      */
     val res2 = pair.map(t =>{
       val random = new Random()
       val key = random.nextInt(2)+"_"+t._1
       (key,t._2)
     }).groupByKey()
      .flatMap(t =>{
        val name = t._1
        val top2 = t._2.toList.sorted.takeRight(2).reverse
       top2.map(i =>{
         (name,i)
       })
      }).map(t =>{
       val key = t._1.split("_")(1)
       val value = t._2
       (key,value)
     }).groupByKey()
      .map(t =>{
        val name = t._1
        val top2 = t._2.toList.sorted.takeRight(2).reverse
        (name,top2)
      })
    println("=======第二种方式========")
    res2.collect().foreach(println)

    /**
      * 第3种
      * map端预聚合，作用相当于combiner，在每个map先进行一次本地的reduce操作
      * 优点： 只进行一次shuffl，但是没有数据倾斜
      */

    /**
      * def aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U)
      *  (zeroValue: U) 为每个map中相同的key先进行一次聚合操作的初始值，对于wordcount而言，就是0，对于当前案例那就是ArrayBuffer或者是ListBuffer
      * seqOp: (U, V) => U 在每个map中为相同key的vlaue进行的聚合操作，对于wordcount而言，0+1+1...,对于当前案例,就是ArrayBuffer或者是ListBuffer添加value
      * combOp: (U, U) => U 在redcue阶段，为已经聚合过的每个map中相同key的vlaue再进行的聚合操作，对于wordcount而言n+n+...,对于当前案例,就是合并多个map聚合之后的ArrayBuffer或者是ListBuffer添
      */

    // val wc = pair.aggregateByKey(0f)(_+_,_+_)
    val res3 = pair.aggregateByKey(new ArrayBuffer[Float]())(
      (u1,v1) =>{
        u1 += v1
        u1.sorted.takeRight(2)
      },
      (u1,u2)=>{
        u1 ++= u2
        u1.sorted.takeRight(2).reverse
      }
    )
    println("=======第三种方式========")
    res3.collect().foreach(println)

  }
}
