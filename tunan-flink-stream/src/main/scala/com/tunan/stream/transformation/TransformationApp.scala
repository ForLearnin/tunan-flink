package com.tunan.stream.transformation

import com.tunan.stream.bean.{Access, MySQLSource, Student, Traffic}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object TransformationApp {



    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //        aggregations(env)
        //        union(env)
        //        collect(env)
        env.execute(this.getClass.getSimpleName)
    }

    // 支持相同类型
    private def union(env: StreamExecutionEnvironment) = {
        val student = env.addSource(new MySQLSource)
        val file = env.readTextFile("tunan-flink-stream/data/access.txt")

        val access = file.map(row => {
            val words = row.split(",").map(_.trim)
            Access(words(0).toLong, words(1), words(2).toLong)
        })

        student.union(student).map(x => x).print()
        //        student.union(access).map(x => x).print()  // 报错
    }

    // 支持不同类型
    private def collect(env: StreamExecutionEnvironment) = {
        val student: DataStream[Student] = env.addSource(new MySQLSource).setParallelism(1)
        val file = env.readTextFile("tunan-flink-stream/data/access.txt")

        val access: DataStream[Access] = file.map(row => {
            val words = row.split(",").map(_.trim)
            Access(words(0).toLong, words(1), words(2).toLong)
        })
        student.connect(access).map(a => a, b => b).print()

    }

    private def aggregations(env: StreamExecutionEnvironment) = {
        val file = env.readTextFile("tunan-flink-stream/data/student.txt")
        //        file.map((_,1)).keyBy(_._1).min(1).print()
        file.map(x => {
            val words = x.split(",").map(_.trim)
            (words(0), words(1).toInt)
        }).keyBy(_._1)
          //          .min(1)
          //          .max(1)
          //          .sum(1)
          .print()
    }
}
