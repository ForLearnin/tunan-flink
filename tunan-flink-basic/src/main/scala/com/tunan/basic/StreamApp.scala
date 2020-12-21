package com.tunan.basic

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable

object StreamApp {

    val CONFIG_FILE_PATH = ""

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val stream = env.socketTextStream(getParameters("host"), getParameters("port").toInt)
        println(stream.parallelism)
        stream
          .flatMap(_.toLowerCase.split(","))
          .filter(_.nonEmpty)
          .map((_, 1))
          .keyBy(_._1)
          .sum(1)
          .print()
          .setParallelism(1)

        env.execute(getClass.getCanonicalName)
    }

    private def getParameters: mutable.Map[String, String] = {
        val map = mutable.HashMap[String,String]()
        val tool = ParameterTool.fromPropertiesFile(CONFIG_FILE_PATH)
        val host = tool.getRequired("host")
        val port = tool.getRequired("port")
        map += ("host" -> host)
        map += ("port" -> port)
        map
    }
}