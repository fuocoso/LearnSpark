package com.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

   val conf = new SparkConf()
      .setAppName("WordCount")   //设置应用程序的名称，会在4040的监控页面上有显示
      //.setAppName(getClass.getSimpleName)
      .setMaster("local[2]")  //指定应用程序在哪儿运行，可以是local，yarn也可以是standalone（spark://hostname:7071）,注意，打包运行的时候，必须注释掉这行代码
     //.setIfMissing("spark.master","local[2]")

    val sc = SparkContext.getOrCreate(conf)   //每个spark应用程序只能有一个SparkContext对象，spark-shell会自动创建
    /**
      * 作用：
      *   1.指定应用程序向哪个集群资源管理节点提交，决定当前应用程序运行的环境
      *   2.创建初始RDD，还可以用来创建 广播变量 和 累积器等共享变量
      *   3. 创建DagScheduler，划分stage，提交taskeset到taskScheduker，负责由于shuffle导致失败重启
      *   4. 创建TaskScheduler，负责提交askeset到到executor提交，负责处理非shuffle原因的失败重启
      */


    // --------------------spark应用程序开发3步骤-------------
    //第一步：使用sc创建获取读取外部文件形成RDD
    val path = "data/wc.txt"
    val text = sc.textFile(path)
    /**
      * 如果在当前项目的源码(resources)中没有导入 core-site.xml和hdfs-site.xml文件，读取的就是本地文件
      * 这个时候想要读取hdfs的文件，必须加上hdfs的schema信息：hdfs://namenode:8020
      */

     //先在Driver端创建一个变量（可以被序列化）
     val a = Array("1","2","3","4","5","6","7","8","9","10","12")

   //创建累加器
   val  ac = sc.longAccumulator("dity data counter")

   //再创建广播变量，用来广播上面的a
   val bc: Broadcast[Array[String]] = sc.broadcast(a)


  //第二步：调用RDD的transformation完成业务需求
    val filter = text.filter(line => line.nonEmpty)
    val word = filter.flatMap(line => line.split(" ")).filter(i => !bc.value.contains(i))
    val pairs = word.map(word =>(word,1))

    val test = filter.flatMap(_.split(" ")).foreach(x =>{
     if(bc.value.contains(x)){
      ac.add(1)
     }
    })

  System.err.println(ac.value)

    val wc = pairs.reduceByKey((a, b) => a+b)

  //第三步：调用RDD的111111action算子，触发job的提交,对结果进行打印或输出到外部存储系统
    wc.collect().foreach(println)

   //使用完成，要记得销毁
   bc.destroy()

   Thread.sleep(300000)

      //关闭SparkContext对象
    sc.stop()

  }
}
