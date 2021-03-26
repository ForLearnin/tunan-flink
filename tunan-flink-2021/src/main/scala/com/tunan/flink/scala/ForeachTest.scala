package com.tunan.flink.scala

import scala.collection.mutable.ListBuffer

object ForeachTest {

  def main(args: Array[String]): Unit = {

    val names = ListBuffer[String]("zs","ls","ww")

    val context = for {
      name <- names if name.equalsIgnoreCase("zs")

    } yield name + "是个学生"


    context.foreach(println)

  }
}