package com.tunan.basic

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object BatchApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment
        val text: DataSet[String] = env.readTextFile("hdfs:///data/word.txt")
        text
          .flatMap(_.toLowerCase.split(","))
          .filter(_.nonEmpty)
          .map((_, 1))
          .groupBy(0)
          .sum(1)
          .print()

//        env.execute(getClass.getCanonicalName)
    }
}
