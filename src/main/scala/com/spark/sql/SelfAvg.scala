package com.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


object SelfAvg extends UserDefinedAggregateFunction{
  /**
    * 指的是要对表中的哪一列数据就行取平均值操作
    * @return
    */
  override def inputSchema: StructType = StructType(
    //Seq(StructField("salary",FloatType,true))
  //  Array( StructField("salary",FloatType,true))
    StructField("salary",FloatType,true)::Nil
  )

  /**
    * 就是每个分区中用来计算sum 以及对应个数count 的两个缓存变量，可以理解为一张有两列数据的表格，有两列(strucfField)
    * @return
    */
  override def bufferSchema: StructType = StructType(
    Seq(StructField("sum",DoubleType,true),StructField("count",LongType,true))
    //  Array( StructField("salary",FloatType,true))
    //StructField("salary",FloatType,true)::Nil
  )

  /**
    * 调用改函数最终的返回值的类型，其实最后的平均值的类型
    * @return
    */
  override def dataType: DataType = DoubleType

  /**
    *最终结果的一致性：多次执行取平均值，对于相同的输入，得出的结果总是相同
    * @return
    */
  override def deterministic: Boolean = true

  /**
    * 每个分区中缓存变量的初始值，在这里，求和的sum值只能是0.0，求个数的count值只能是0
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //设定的buffer的sum的初始值，是buffer中下标为0的列，其初始值值为0.0
    buffer.update(0,0.0)
    //设定的buffer的count的初始值，是buffer中下标为1的列，其初始值为0.0
    buffer.update(1,0l)
  }

  /**
    * 在每个分区中定义 缓存变量的更新方式
    *  buffer代表的是截至当前行的累计sum值  buffer.sum+当前行的值  buffer.count +1
    *  input代表的当前行的值
    * @param buffer
    * @param input
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //先获取buffer中的sum和count的值
    val sum1 = buffer.getDouble(0)
    val count1 = buffer.getLong(1)

    //再获取当前行的值
    val value = input.getFloat(0)

    //更新
    buffer.update(0,sum1+value)
    buffer.update(1,count1+1)
  }

  /**
    * Merges two aggregation buffers and stores the updated buffer values back to `buffer1`.
    * 将多个分区中各自的缓存值进行更新操作，并将更新后的结果赋值给第一个buffer
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //先获取第一个分区中的buffer对应的sum和count
    val sum1 = buffer1.getDouble(0)
    val count1 = buffer1.getLong(1)

    //再取第一个分区中的buffer对应的sum和count
    val sum2 = buffer2.getDouble(0)
    val count2 = buffer2.getLong(1)

    //合并两个分区中的缓存变量到第一个中
    buffer1.update(0,sum1+sum2)
    buffer1.update(1,count1+count2)
  }

  /**
    * 求值：所有buffer中最终的sum / count
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    val sum = buffer.getDouble(0)
    val count = buffer.getLong(1)
    sum/count
  }
}
