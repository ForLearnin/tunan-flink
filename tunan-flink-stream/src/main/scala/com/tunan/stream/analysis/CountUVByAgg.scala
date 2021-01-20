package com.tunan.stream.analysis

import java.sql.Timestamp
import java.time.Duration

import com.tunan.stream.bean.{AccessPage, UVCount}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object CountUVByAgg {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 2.12后默认就是这个
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //        env.setParallelism(1)

        env.readTextFile("tunan-flink-stream/data/log.txt")
          .map(x => {
              val splits = x.split(",")
              AccessPage(splits(0), splits(1), splits(2), splits(3).toLong)
          })
          .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))
            .withTimestampAssigner(new SerializableTimestampAssigner[AccessPage] {
                override def extractTimestamp(element: AccessPage, recordTimestamp: Long): Long = element.ts
            }))

          .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
          // 这个聚合操作在多个Task上执行
          .aggregate(new AggregateFunction[AccessPage, Set[String], Long] {
              override def createAccumulator(): Set[String] = Set[String]()

              override def add(value: AccessPage, accumulator: Set[String]): Set[String] = accumulator + value.userId

              override def getResult(accumulator: Set[String]): Long = accumulator.size.toLong

              override def merge(a: Set[String], b: Set[String]): Set[String] = ???
              // 输出window的结果
          }, new AllWindowFunction[Long, UVCount, TimeWindow] {
              override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UVCount]): Unit = {
                  out.collect(UVCount(new Timestamp(window.getEnd).toString, input.head))
              }
          }).print()

        env.execute(this.getClass.getSimpleName)
    }
}