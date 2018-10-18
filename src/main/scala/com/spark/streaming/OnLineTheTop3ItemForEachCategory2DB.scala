package com.spark.streaming

import java.sql.{Connection, DriverManager}

import kafka.serializer.StringDecoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017/12/2.
  */
/***
  * 使用SparkStreaming +  SparkSql来在线动态计算电商中不同类型中最热门商品排名，例如手机这个类别下面最热门的三种手机，电视这个类型下最人们的三种电视，该实例在实际生产环境下具有非常中大的意义
  * 实现技术：Spark Streamng + Spark Sql，可以利用spark straming的foreachRDD 和 Tranform等接口，在这些接口中其实是基于RDD进行操作的，所以以RDD为基石，就可以直接使用Spark其他的功能，就像直接调用API一样简单
  *
  */
object OnLineTheTop3ItemForEachCategory2DB {
  def main(args: Array[String]): Unit = {
    /**
      * 第1步：创建SparkConf对象，并且设置程序运行时需要连接到Spark集群的Master Url以及应用名称，
      * 如果设置为local,则代表程序在本地运行
      */
    val spark = SparkSession.builder()
        .appName("OnLineTheTop3ItemForEachCategory2DB")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions","5")
       .getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc,Seconds(5))

    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "linux01:9092",
      "auto.offset.reset"-> "smallest"
    )

    val topics = Set("click")

    val userClickLogsDStream = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

    val formattedUserClickDstream = userClickLogsDStream
      .filter(line => line.nonEmpty)
      .map(line => line.split(","))
      .filter(arr => arr.length == 3)
      .map(arr => (arr(1)+"_"+arr(2),1))

    formattedUserClickDstream.print(3)

    val categoryUserClickLogDstream: DStream[(String, Int)] = formattedUserClickDstream
      .reduceByKeyAndWindow(
        _+_,
        _-_,
        Seconds(60),
        Seconds(50)
      )

    categoryUserClickLogDstream.print(3)

    categoryUserClickLogDstream.foreachRDD(rdd =>{
      if(rdd.isEmpty()){
        println("No data inputted")
      }else{
        //(HuaWei_p20,5905)(Oppo_Findx,3672)
        val categoryitemRow = rdd.map(reduceItem =>{
          val category = reduceItem._1.split("_")(0)
          val item = reduceItem._1.split("_")(1)
          val click_count = reduceItem._2
          Row(category,item,click_count)
        })

        val structType = StructType(Array(
          StructField("category",StringType,true),
          StructField("item",StringType,true),
          StructField("click_count",IntegerType,true)
        ))

        val categoryItemDF = spark.createDataFrame(categoryitemRow,structType)

        categoryItemDF.createOrReplaceTempView("categoryItemTable")
       // hiveContext.cacheTable("categoryItemTable")

        //打印3条数据
        spark.sql("select * from categoryItemTable").show(3)


        val resultDataFrame = spark.sql(
          """
            |select
            |a.category,a.item,a.click_count
            |from(
            |  select
            |  category,item,click_count,
            |  row_number() over(partition by category order by click_count desc) rank
            |  from
            |  categoryItemTable
            |  ) a
            | where
            | a.rank <=3
          """.stripMargin)
        //打印6条记录
        resultDataFrame.show(6)

        val resultRowRdd = resultDataFrame.rdd

       println(resultRowRdd.partitions.size)

        resultRowRdd.foreachPartition(partitionOfRecords =>{
          if(partitionOfRecords.isEmpty){
            println("This RDD is not null but partition is null")
          }else{
            val url = "jdbc:mysql://localhost:3306"
            val username = "root"
            val password = "root123"
            Class.forName("com.mysql.jdbc.Driver")
           var connection:Connection = null
            try{
              connection = DriverManager.getConnection(url,username,password)
              connection.setAutoCommit(false)
              val sql = "insert into mydb.categoryItem (category,item,click_count)  values(?,?,?)"
              val pstmt = connection.prepareStatement(sql)
              var count = 0
              partitionOfRecords.foreach(record=>{
                pstmt.setString(1,record.getAs("category"))
                pstmt.setString(2,record.getAs("item"))
                pstmt.setInt(3,record.getAs("click_count"))
                pstmt.addBatch()
                count +=1
                if(count % 500 ==0){
                  pstmt.executeBatch()
                  connection.commit()
                }
              })
             pstmt.execute()
              connection.commit()
            }finally {
              connection.close()
            }

          }
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
