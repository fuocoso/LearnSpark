package com.spark.StructStreaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.StructType

object KafkaStrStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Struct Streaming ETL for Json")
      .setIfMissing("spark.master","local[2]")
      .set("spark.sql.shuffle.partitions","1")

   val spark = SparkSession.builder().config(conf).getOrCreate()
import spark.implicits._
   val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","linux01:9092")
      .option("subscribe","order")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[(String)]

    import spark.sql

    val schema = StructType(Nil)
      .add("date","String")
      .add("userid","String")
      .add("brand","String")
      .add("product","String")
      .add("price","Float")
      .add("age","Int")
      .add("male","String")

    import org.apache.spark.sql.functions._
    val query = lines.select(get_json_object(col("value"),"$.date").alias("date"),get_json_object(col("value"),"$.userid").alias("userid"),get_json_object(col("value"),"$.brand").alias("brand"),get_json_object(col("value"),"$.product").alias("product"),get_json_object(col("value"),"$.price").alias("price"),get_json_object(col("value"),"$.age").alias("age"),get_json_object(col("value"),"$.male").alias("male"))

    query.createTempView("order")

    val queryTab = sql("select userid,product,price,age,male from order")
      .groupBy(
            col("userid")
          ).agg("price"->"count")
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(ProcessingTime(5000))
      .start()

    queryTab.awaitTermination()

  }
}
