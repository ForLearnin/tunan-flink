package com.tunan.kafka.stream

import com.tunan.kafka.sink.RedisSink
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TrafficsApp {
    def main(args: Array[String]) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val parameters = ParameterTool.fromPropertiesFile("tunan-flink-kafka/conf/parameters.properties")
        env.getConfig.setGlobalJobParameters(parameters)
        val stream = env.fromElements(
            "20211001,gd,sz,a.com,1000",
            "20211001,gd,sz,b.com,3000",
            "20211001,gd,gz,a.com,1000",
            "20211001,gd,gz,b.com,3000",
            "20211001,gd,gz,b.com,2000"
        ).map(x => {
            val splits = x.split(",").map(_.trim)
            Log(splits(0),splits(1),splits(2),splits(3),splits(4).toLong)
        })

        stream
          .keyBy(x => (x.day,x.province,x.domain))
          .sum("traffic")
          .map(x => {
              (s"day_province_domain_cnt-${x.day}-${x.province}",x.domain,x.traffic.toInt)
          }).addSink(new RedisSink)

        env.execute(this.getClass.getSimpleName)
    }
}

case class Log(day: String, province: String, city: String, domain: String, traffic: Long)