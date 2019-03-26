package com.learn.core

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object AddItem {
  def main(args: Array[String]): Unit = {
    val a = new ArrayBuffer[(String,Int)]()
    val b = ("jack",6)

    val c =ArrayBuffer.apply(b)
    a ++= c
  print(a)

  }

}
