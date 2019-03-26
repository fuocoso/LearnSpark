package com.spark.core

import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}

object DSWordCount {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession
      .builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    import  spark.implicits._
    val data  = spark.read.text("data/dept.txt").as[String]

    val words = data.flatMap(_.split("\t"))

  val tds: Dataset[(Int, String, String)] =   data.map(line =>{
     val arr =  line.split("\t")
      (arr(0).toInt,arr(1),arr(2))
    })

    tds.createTempView("a")
    import spark.sql
    sql("select count(_1),max(_1),min(_1)  from a").show()


     tds.select($"_1".as("id"),$"_2".as("name"),$"_3".as("loc"))


  val empText = spark.sparkContext.textFile("data/emp.txt")

   val empRdd = empText.map(line =>{
      val arr = line.split("\t")
      val id:Int = if (arr(0).nonEmpty) arr(0).toInt else 0
      val empname = arr(1)
      val job = arr(2)
      val mgid:Int = if(arr(3).nonEmpty) arr(3).toInt else 0
      val hireDate = arr(4)
      val salary:Float = if(arr(5).nonEmpty) arr(5).toFloat else 0.0F
      val bouns:Float = if(arr(6).nonEmpty) arr(6).toFloat else 0.0F
      val deptid:Int = if (arr(7).nonEmpty) arr(7).toInt else 0
      emp(id,empname,job,mgid,hireDate,salary,bouns,deptid)
    })
  val empDF = empRdd.toDF()
    empDF.show()

   val empDS =  empDF.map(row =>{
      val id = row.getInt(0)
      val name = row.getString(1)
      val hireDate = row.getString(4)
      (id,name,hireDate)
    })
    empDS.show()
  }
}
case class emp(empid:Int,empname:String,job:String,mgid:Int,hireDate:String,salary:Float,bonus:Float,deptid:Int)