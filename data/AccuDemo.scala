package com.learn.core

import com.learn.core.BroadcastDemo.getClass
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AccuDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(getClass.getTypeName)
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)
    //2-1.创建累加器
    val accu1 = sc.longAccumulator("counter")
    val accu2 = sc.longAccumulator("sum")

    val num = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)

    val rdd = sc.parallelize(num)

    //    val res =  rdd.foreach(i =>{
    //      if (i % 3 ==0)
    //        accu1.add(1)
    //    })

    val res = rdd.map(i => {
      if (i % 3 == 0)
      //累加器用来计数的
        accu1.add(1)
      i
    })

    val res1 = rdd.map(i =>{
      //累加器用来求和
      accu2.add(i)
      (i,1)
    })
    //执行一次的结果是：6,一旦RDD的数据被缓存，cache是懒加载的过程，触发一次job才会正真缓存
    res.cache().count()
    //执行二次的结果是：6，前一步已经缓存了，直接在缓存中拿取数据，不再执行了
    res1.collect()
    //执行一次的结果是：2
    res.take(7)

    println("1到20中的被3整除的个数:" + accu1.value)

    println("1到20的和:" + accu2.value)
  }

}
