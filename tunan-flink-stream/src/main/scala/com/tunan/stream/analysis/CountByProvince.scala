package com.tunan.stream.analysis


import java.sql.Timestamp

import com.tunan.stream.bean.{AccessPage, CountByProvince}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object CountByProvince {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 2.12后默认就是这个
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        env.readTextFile("tunan-flink-stream/data/log.txt")
          .map(x => {
              val splits = x.split(",")
              AccessPage(splits(0), splits(1), splits(2), splits(3).toLong)
          })
          // 2.12的最新用法
          //          .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))
          //            .withTimestampAssigner(new SerializableTimestampAssigner[AccessPage] {
          //              override def extractTimestamp(element: AccessPage, recordTimestamp: Long): Long = element.ts
          //          }))
          .keyBy(x => x.province)
          .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
          .aggregate(new AggregateFunction[AccessPage, Long, Long] {
              override def createAccumulator(): Long = 0L

              override def add(value: AccessPage, accumulator: Long): Long = accumulator + 1

              override def getResult(accumulator: Long): Long = accumulator

              override def merge(a: Long, b: Long): Long = a + b
          }, new WindowFunction[Long, CountByProvince, String, TimeWindow] {
              override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
                  out.collect(com.tunan.stream.bean.CountByProvince(new Timestamp(window.getEnd).toString, key, input.head))
              }
          })
          .print()

        env.execute(this.getClass.getSimpleName)
    }
}
