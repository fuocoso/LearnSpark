package com.spark.sql

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object Dataframe2RDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Dataframe2RDD")
      .setIfMissing("spark.master","local[2]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    /**
      * Dataframe 转RDD
      *   凡是Hive SQL可以转成MR来执行，但是反过来是不成立，一些复杂业务需求依然还是需要通过写MR实现
      *   同理，dataframe和rdd也是相同的关系，在一些使用dataframe没有办法处理的情况，就需要用rdd来处理
      *   如果处理完的datafrma数据要保存，比如说保存到jdbc，提供的写入模式，就只有追加append 覆盖overwrite，显然是不够用的，
      *   然是如果转成RDD之后，就可以时候用原生的JDBC代码来处理，可以使用 insert alter来插入和更新数据，或者使用现有的一些数据库连接框架来处理，比如herbernate，mybaties
      */

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


    //方式一：
    val rdd1: RDD[Row] = empDF.rdd

    val res: RDD[(Int, String, Float)] = rdd1.map(row =>{
      val id = row.getAs[Int](0)  //表中的id
      val hiredate = row.getString(4)  //表中hiredate列
      val  salary = row.getAs[Float]("salary") //通过列名来获取，要要求必须是传入列名
//      id+","+hiredate+","+salary
//      s"$id,${hiredate},${salary}"
      (id,hiredate,salary)
    })

    /**
      * res.foreachPartition()  和 res.foreach()
      * res.mapPartitions() 和 res.map()
      * 以上两组的写法的功能完全一致，那么有没有区别？
      * 带有Partitions的算子，一般叫做高性能算子
      * 其实map和mapPartitions功能上一样的，但是区别
      * map针对的是RDD中每一个元素，换言之，就是RDD一个元素会调用一次map
      * mapPartitions针对的是RDD每一个分区，换言之，就是RDD一个分区会调用一次map
      */

    res.foreachPartition(iter =>{
     // iter.foreach(println(_))
      val url = "jdbc:mysql://localhost:3306"
      val username = "root"
      val password = "root123"
      //4.1.1、jdbc驱动的添加
      Class.forName("com.mysql.jdbc.Driver")
      //4.1.2、Connection连接对象获取并设置自动提交强制关闭
      var connection:Connection = null
      try{
        connection = DriverManager.getConnection(url,username,password)
        connection.setAutoCommit(false)
        //4.1.3、prearedStatement对象获取
        val sql =
          """
            |insert into mydb.rdd2mysql(id,name,salary) values(?,?,?)
          """.stripMargin
        val pstmt =connection.prepareStatement(sql)
        var count = 0
        //4.1.4、保存到数据库（批量提交）
        iter.foreach(item =>
        {
          pstmt.setInt(1,item._1)
          pstmt.setString(2,item._2)
          pstmt.setFloat(3,item._3)

          pstmt.addBatch() //添加到批次中
          count += 1

          if(count % 500 == 0){
            //500条数据进行一次提交操作
            pstmt.executeBatch()
            connection.commit()
          }
        })
        //提交操作
        pstmt.executeBatch()
        connection.commit()

      }finally {
        //4.1.5、关闭连接
        connection.close()
      }
    })

    res.collect().foreach(println(_))

    //方式二：在spark1.6.x的时候可以，2.0不可以，凡是要处理dataset，必须先导入隐式转化包
    import  spark.implicits._

   val ds: Dataset[(Int, String, Float)] =  empDF.map(t =>{
     val id = t.getAs[Int](0)  //表中的id
     val hiredate = t.getString(4)  //表中hiredate列
     val  salary = t.getAs[Float]("salary") //通过列名来获取，要要求必须是传入列名
     (id,hiredate,salary)
   })

    ds.printSchema()
    ds.select("_2").show(3)

    ds.collect().foreach(println(_))



  }
}
