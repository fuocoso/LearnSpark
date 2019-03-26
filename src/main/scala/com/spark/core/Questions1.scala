package com.spark.Interview

import java.sql.{Connection, Driver, DriverManager}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object Questions1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val texta = spark.sparkContext.textFile("data/ipa.txt")
    val textb = spark.sparkContext.textFile("data/ipb.txt")


    val rdda = texta.map(line => {1
      val arr = line.split(",")
      (arr(0), arr(1))
    })

    val rddCase = texta.map(line => {1
      val arr = line.split(",")
      IPLog(arr(0), arr(1))
    })
    import spark.implicits._
    val df = rddCase.toDF

    df.write.format("parquet").partitionBy("ip").mode(SaveMode.Overwrite).saveAsTable("mydb.ipa")


    val rddb = textb.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1))
    })

    /**
      * 需求一：每个文件中的ip数
      */
    val counta = rdda.map(_._1).distinct().count()
    val countb = rddb.map(_._1).distinct().count()

    System.err.println(s"第1个文件中的ip数：$counta")
    System.err.println(s"第1个文件中的ip数：$countb")

    /**
      * 需求二：出现在ipb.text而没有出现在ipa.text的ip
      */
    val ipa = rdda.map(_._1).distinct()
    val ipb = rddb.map(_._1).distinct()

    //先求两个文件中共同的ip
    val com = ipa.intersection(ipb)
    System.err.println(s"两个文件中都有：")
    com.foreach(println(_))

    //再求b中有但是a中没有的
    val onlya = ipa.subtract(com)
    System.err.println(s"a中有但是b中没有的ip：")
    onlya.foreach(println(_))

    /**
      * 需求三：每个user出现的次数以及每个user对应的IP的个数
      */
    //a中每个user出现的次数
    val ipAwc = rdda.map(t => (t._1,1)).reduceByKey(_+_)

    //b中每个user出现的次数
    val ipBwc = rddb.map(t =>(t._1,1)).reduceByKey(_+_)

    //两个文件中合计出现的次数
    val wc = ipAwc.union(ipBwc).reduceByKey(_+_)
    System.err.println(s"a中每个user出现的次数：${ipAwc.foreach(print(_))}")
    System.err.println(s"b中每个user出现的次数：${ipBwc.foreach(print(_))}")
    System.err.println(s"b中每个user出现的次数：${wc.foreach(print(_))}")

    /**
      * 需求四：对应IP数最多的前K个user
      */
   val res =  rdda.union(rddb).map(t => (t,1)).reduceByKey(_+_).map(t => (t._1._1,(t._1._2,t._2))).groupByKey().map(t=>{
     val ip = t._1
     val sort = t._2.toList.sortBy(_._2).takeRight(3).reverse
     (ip,sort)
   })
    res.foreach(println)

    rddb.foreachPartition(iter =>{
      val url = "jdbc:mysql://localhost:3306/mydb"
      val user = "root"
      val password = "root123"

      Class.forName("com.mysql.jdbc.Driver")

      var con :Connection =null
      try {
        con = DriverManager.getConnection(url,user,password)
        con.setAutoCommit(false)
        val sql = "replace into ipb(ip,name,id) values(?,?,?)"
        val ptst = con.prepareStatement(sql)

        var count =1
        iter.foreach(t=>{
          ptst.setString(1,t._1)
          ptst.setString(2,t._2)
          ptst.setInt(3,count)
          ptst.addBatch()
          count +=1
          if(count % 500 ==0){
            ptst.executeBatch()
            con.commit()
          }
          ptst.executeBatch()
          con.commit()
        })
      }finally {
        con.close()
      }
    })

  }
}

case class IPLog(ip:String,name:String)