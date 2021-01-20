package com.tunan.stream.watermark

import java.sql.Timestamp

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowByEventTime {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 这里并行度的设置很重要，不然数据不在一个分区内
        env.setParallelism(1)
        // 设置EventTime作为window时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        env.socketTextStream("aliyun", 9999).filter(_.trim.nonEmpty)
          // 拿到EventTime
          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(1)) {
              override def extractTimestamp(element: String): Long = {
                  element.split(",")(0).toLong
              }
          })
          .map(x => {
              val splits = x.split(",")
              // timestamp,word,1
              (splits(1).trim, splits(2).trim.toInt)
          })
          .keyBy(_._1)  // keyBy
          // 定义滑动窗口
          .window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(3)))
          // 实现reduce函数
          .reduce(new ReduceFunction[(String, Int)] {
              // 对相同key的数据进行reduce操作
              override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
                  (value1._1, value2._2 + value1._2)
              }
              // 实现window函数，window中有这个窗口的所有reduce操作后的数据
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
