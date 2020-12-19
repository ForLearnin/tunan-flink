package com.tunan.basic

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val stream = env.socketTextStream("aliyun", 9999)
        stream
          .flatMap(_.toLowerCase.split(","))
          .filter(_.nonEmpty)
          .map((_, 1))
          .keyBy(_._1)
          .sum(1)
          .print()

        env.execute(getClass.getCanonicalName)
    }
}