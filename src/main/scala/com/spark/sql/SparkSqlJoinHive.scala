package com.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSqlJoinHive {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf()
      .setAppName("Spark SQL join Nysql")
      .setIfMissing("spark.master","local[2]")
      .set("spark.sql.shuffle.partitions","200")
     // .setMaster("local[2]")


    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.autoBroadcastJoinThreshold","10485760")
      .enableHiveSupport()
      .getOrCreate()

    //完成第一步，读取hive中表，然后保存到数据库中
    val url = "jdbc:mysql://localhost:3306/mydb"
    val table = "from_hive_emp"
    val prop = new Properties()
    prop.setProperty("driver","com.mysql.jdbc.Driver")
    prop.setProperty("user","root")
    prop.setProperty("password","root123")

    spark
      .read
      .table("mydb.emp")
      .write
      //.mode("overwrite")
      .mode(SaveMode.Ignore)
      .jdbc(url,table,prop)

    //分别读取hive中dept 以及 mysqk中的 from_hive_emp

    val dept = spark.table("mydb.dept").createTempView("a")

    val emp = spark.read.jdbc(url,table,prop).createTempView("b")


    //完成join操作
    val res = spark.sql(
      """
        |select
        |  empname,salary,bonus,salary+nvl(bonus,0.0) tal,location
        |from
        |  a
        |join
        |  b
        |on a.deptid=b.deptid
      """.stripMargin)

    //打印和保存
    res.show()
   // res.write.save("hdfs://linux01:8020/spark/output")








  }
}
