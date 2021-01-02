package com.tunan.stream.state


import com.tunan.stream.bean.Access
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SimpleDefinedState {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hadoop")

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val files = env.readTextFile("tunan-flink-stream/data/access.txt")

        val result = files.map(row => {
            val words = row.split(",").map(_.trim)
            Access(words(0).toLong, words(1), words(2).toLong)
        }).keyBy(x => (x.time, x.domain))

        env.execute(this.getClass.getSimpleName)

    }
}
