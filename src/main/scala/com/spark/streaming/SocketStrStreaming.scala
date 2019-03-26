package com.spark.StructStreaming

import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SocketStrStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setIfMissing("spark.master", "local[2]")
      .set("spark.sql.shuffle.partitions", "1")


    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", "9999")
      .load()


    val words = lines.as[String].flatMap(_.split("\t"))

    println("是否为流：" + words.isStreaming)

    val schema = StructType(Nil)
      .add("word", "String")

    //使用window窗口函数必须导这个隐式转换包
    import org.apache.spark.sql.functions._

    val res = words.select($"value".alias("word"))

    val wordcount = res.select("word")
//      .groupBy(
//        $"word"
//      ).count()
    //   window($"word", "10 seconds", "5 seconds"),

    val query = wordcount.writeStream
      .format("json")
     // .option("checkpointLocation","data/to/checkpoint")
      .option("path", "data/wc/out")
      .start()

    query.awaitTermination()
  }
}
