package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object SparkReadJson {
  def main(args: Array[String]): Unit = {
    //spark2.x开始 Spark SQL入口变成了SparkSeesion，合并原来的sqlContext和hiveContext，但是做了保留
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Read json and Hive")
      .enableHiveSupport()    //如果要读取hive的表，就必须使用这个
      .getOrCreate()





    //演示spark1.x的入口
//    val conf = new SparkConf()
//      .setIfMissing("spark.master","local[2]")
//      .setAppName(getClass.getSimpleName)
//
//    val sc = SparkContext.getOrCreate(conf)
//
//    val sqlContext = new SQLContext(sc)
//
//    sqlContext.sql("select * from mydb.emp").show

    //第一步：通过sparkSession读取外部数据源形成初始的dataframe

    val df: DataFrame = spark.read.json("hdfs://linux01:8020/spark/resources/people.json")

    df.createTempView("person")

    spark.sql("select name,age from person where age >21").show

    //第二步，使用sql或者是dsl实现统计查询
    //*** 今后凡是写spark sql，就第一时刻导入隐式转化包
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //采用DSL来开发spark sql
    // select name from person
    df.select("name").show()
//    df.select($"name").show()
//    df.select(df("name")).show()
//    df.select(col("name")).show
//    df.selectExpr("name")

    //select name,age+1 from person
    df.select($"name",$"age"+1)

    //select name,age from person where age >21
    df.select(col("age")>21)

    //select age,count(1) from person group by age
    df.groupBy("age").count()


    //第三步，打印或者保存数据
    df.show()

    df.
      write
      .mode("overwrite")
      .partitionBy("age")
      .saveAsTable("mydb.person")

    //===================================================
    val emp = spark.read.table("mydb.emp")

  spark.sql("select * from mydb.emp").show

    emp.write
      .mode("append")
      .bucketBy(3,"deptid")
      .saveAsTable("mydb.bucket_Emp")

    emp.write
      .mode("append")
      .save("/spark/test")





  }
}
