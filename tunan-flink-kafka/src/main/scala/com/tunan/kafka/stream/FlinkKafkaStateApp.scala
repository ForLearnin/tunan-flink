package com.tunan.kafka.stream

import com.tunan.kafka.sink.RedisSink
import com.tunan.kafka.utils.FlinkKafkaUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

object FlinkKafkaStateApp {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val parameters: ParameterTool = ParameterTool.fromPropertiesFile("tunan-flink-kafka/conf/parameters.properties")

        val stream: DataStream[String] = FlinkKafkaUtils.createKafkaSource(parameters)

        val result = stream
          .flatMap(_.split(","))
          .map((_, 1))
          .keyBy(0)
          .sum(1)
          .map(x => ("wc", x._1, x._2))

        result.print()
        result.addSink(new RedisSink)

        FlinkKafkaUtils.getEnv.execute(this.getClass.getSimpleName)
    }
}
