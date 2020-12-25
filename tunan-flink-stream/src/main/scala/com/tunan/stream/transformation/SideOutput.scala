package com.tunan.stream.transformation

import com.tunan.stream.bean.Traffic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSink

object SideOutput {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        process(env)

        env.execute(this.getClass.getSimpleName)
    }
    def process(env: StreamExecutionEnvironment): DataStreamSink[Traffic] = {
        val file: DataStream[String] = env.readTextFile("tunan-flink-stream/data/traffic.txt")
        val traffic: DataStream[Traffic] = file.map(row => {
            val words = row.split(",")
            Traffic(words(0), words(1), words(2).toLong)
        })
        val hunan = new OutputTag[Traffic]("湖南省分流")
        val guangdong = new OutputTag[Traffic]("广东省分流")
        val changsha = new OutputTag[Traffic]("长沙市分流")
        val huaihua = new OutputTag[Traffic]("怀化市分流")

        // 1.12版本废弃了
        val markFlow = traffic.process(new ProcessFunction[Traffic, Traffic] {
            override def processElement(value: Traffic, ctx: ProcessFunction[Traffic, Traffic]#Context, out: Collector[Traffic]): Unit = {

                if (value.province == "湖南省") {
                    ctx.output(hunan, value)
                    if(value.city == "长沙市"){
                        ctx.output(changsha, value)
                    } else if(value.city == "怀化市"){
                        ctx.output(huaihua, value)
                    }
                } else if (value.province == "广东省") {
                    ctx.output(guangdong, value)
                }
            }
        })
        //        markFlow.getSideOutput(guangdong).print("广东省分流")
        markFlow.getSideOutput(hunan).print("湖南省分流")
        markFlow.getSideOutput(changsha).print("长沙市分流")
        markFlow.getSideOutput(huaihua).print("怀化市分流")

    }
}
