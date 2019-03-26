package com.learn.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TestSpark {
  def main(args: Array[String]): Unit = {
   val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setIfMissing("spark.master","local[2]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val df: DataFrame = spark.read.json("data/deviceLog.data")

    df.show()
    df.printSchema()

    val ds:Dataset[Row] = df

    println("===dataset的schema===")
   ds.printSchema()



  }
}
