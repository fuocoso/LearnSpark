package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Rdd2Dataframe {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark SQL join Nysql")
      .setIfMissing("spark.master","local[2]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    //val sc = spark.sparkContext
   // val sc = SparkContext.getOrCreate(conf)

    /**
      * RDD 转 Dataframe  原因;将RDD转成datafrmae之后，可以比较方便的使用sql开发，比如可以使用row_number() over()来实现TopN
      * 方式一：通过反射推断schema
      *      要求：RDD的元素类型必须是case class
      * 方式二：编程指定schema
      *     要求：RDD的元素类型必须是Row
      *           自己编写schema（StructType）
      *           调用SparkSession的createDatafrmame（RDD[Row],schema）
      */

    //方式一:通过反射推断schema
    val logRdd: RDD[String] = spark.sparkContext.textFile("data/HTTP.data")
    val HttpRDD: RDD[HttpLog] = logRdd.map(line =>{
      val arr: Array[String] = line.split("\t")
      val time = arr(0).trim.toLong
      val phone = arr(1)
      val apmac = arr(2)
      val acmac = arr(3)
      val host = arr(4)
      val siteType = arr(5)
      val upPackNum = arr(6).trim.toLong
      val downPackNum = arr(7).trim.toLong
      val upPayLoad = arr(8).trim.toLong
      val downPayLoad = arr(9).trim.toLong
      val state = arr(10)
      HttpLog(time,phone,apmac,acmac,host,siteType,upPackNum,downPackNum,upPayLoad,downPayLoad,state)
    })

    import spark.implicits._
    val HttpDF: DataFrame = HttpRDD.toDF
    HttpDF.show(5)


 //方式二;编程指定schema
    val emp = spark.sparkContext.textFile("data/emp.txt")

    val empRDD: RDD[Row] = emp.map(line =>{
      val arr = line.split("\t")
      val id =  arr(0).trim.toInt
      val name = arr(1)
      val job = arr(2)
      val mgid = if (arr(3).nonEmpty) arr(3).trim.toInt else 0
      val hiredate = arr(4)
      val salary = arr(5).trim.toFloat
      val bonus = if (arr(6).nonEmpty) arr(6).trim.toFloat else 0.0f
      val did = arr(7).trim.toInt
      Row(id,name,job,mgid,hiredate,salary,bonus,did)
    })

    val schema = StructType(
      Array(
        StructField("id",IntegerType,true),
        StructField("name",StringType,true),
        StructField("job",StringType,true),
        StructField("mgid",IntegerType,true),
        StructField("hiredate",StringType,true),
        StructField("salary",FloatType,true),
        StructField("bonus",FloatType,true),
        StructField("did",IntegerType,true)
    )
    )

    val empDF: DataFrame = spark.createDataFrame(empRDD,schema)

    empDF.show()
  }
}
case class HttpLog(time:Long,phone:String,apmac:String,acmac:String,host:String,siteType:String,upPackNum:Long,downPackNum:Long,upPayLoad:Long,downPayLoad:Long,state:String)