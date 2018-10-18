package com.spark.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.math.BigDecimal.RoundingMode

object SparkSQLFunc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Dataframe2RDD")
      .setIfMissing("spark.master","local[2]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()


    //Spark SQL 自定义函数UDF
    spark.udf.register("format_Double",(col:Double,scale:Int)=>{
      import java.math.BigDecimal
      val bd = new BigDecimal(col)
      bd.setScale(scale,RoundingMode.HALF_UP).doubleValue()
    })

    //自定义UDAF
    spark.udf.register("udaf_avg",SelfAvg)

    val url = "jdbc:mysql://localhost:3306/mydb"
    val table = "from_hive_emp"
    val prop = new Properties()
    prop.setProperty("driver","com.mysql.jdbc.Driver")
    prop.setProperty("user","root")
    prop.setProperty("password","root123")

     val empdf = spark
       .read
      .jdbc(url,table,prop)

    empdf.createTempView("a")

    spark.sql(
      """
        |select
        |  deptid,avg(salary) avg1,
        |  format_Double(avg(salary),2) avg2,
        |  udaf_avg(salary) avg3,
        |  format_Double(udaf_avg(salary),2) avg4
        |from
        |  a
        |group by deptid
      """.stripMargin).show()



  }
}
