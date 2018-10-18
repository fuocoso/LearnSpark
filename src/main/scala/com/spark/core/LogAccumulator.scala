package com.spark.core

import java.util

import org.apache.spark.util.AccumulatorV2

class LogAccumulator extends AccumulatorV2[String,java.util.Set[String]]{
  //用来返回最后累加结果的集合
  private val logArray = new util.HashSet[String]()

  //用来判断累加器初始值，对于计数而言就是0，对于一个集合类型的就是空集合
  override def isZero: Boolean = {
    logArray.isEmpty
  }

  //在每个task中创建一个LogAccumulator对象
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc = new LogAccumulator()
    logArray.synchronized{
      newAcc.logArray.addAll(logArray)
    }
    newAcc
  }

  //重置累加器
  override def reset(): Unit = {
    logArray.clear()
  }

  //每个task或者分区内进行数据的累加
  override def add(v: String): Unit = {
    logArray.add(v)
  }

  //合并多个分区的累加结果
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case o:LogAccumulator => logArray.addAll(o.value)
    }
  }

  //对应累加器当前的值
  override def value: util.Set[String] = {
    java.util.Collections.unmodifiableSet(logArray)
  }
}
