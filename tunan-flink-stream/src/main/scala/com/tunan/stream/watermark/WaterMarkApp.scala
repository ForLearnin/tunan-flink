package com.tunan.stream.watermark

import java.sql.Timestamp

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMarkApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        env.socketTextStream("aliyun", 9999).filter(_.trim.nonEmpty)
          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(1)) {
              override def extractTimestamp(element: String): Long = {
                  element.split(",")(0).toLong
              }
          })
          .map(x => {
              val splits = x.split(",")
              // timestamp,word,1
              (splits(1).trim, splits(2).trim.toInt)
          }).keyBy(_._1)
          .window(TumblingEventTimeWindows.of(Time.seconds(3)))
          .reduce(new ReduceFunction[(String, Int)] {
              override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
                  (value1._1, value2._2 + value1._2)
              }
          }, new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
              override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
                  for (ele <- elements) {
                      out.collect(s"窗口开始时间: ${new Timestamp(context.window.getStart)}, " +
                        s"窗口结束时间: ${new Timestamp(context.window.getEnd)}, " +
                        s"Key: ${ele._1}, " +
                        s"Value: ${ele._2}, "
                      )
                  }
              }
          }).print()


        env.execute(this.getClass.getSimpleName)
    }
}
