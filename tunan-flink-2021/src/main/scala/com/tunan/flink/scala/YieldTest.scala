package com.tunan.flink.scala

import java.io.File
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource
import scala.io.Source._

object YieldTest {

    def main(args: Array[String]): Unit = {

        val lines: Iterator[String] = fromURL(getClass.getResource("/word.txt")).getLines()

        val strings = for {
            line <- lines if line.trim.contains("棒")
        }
            // 对循环的结果做进一步的处理
            yield line + " :合计 " + line.length + "个字"

        for (elem <- strings) {
            println(elem)
        }

    }
}