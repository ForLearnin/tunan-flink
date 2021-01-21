package com.tunan.stream.analysis

import java.sql.Timestamp
import java.time.Duration

import com.tunan.stream.bean.{AccessPage, PVCount}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

object CountPVBySkew {

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
          // 分
          .map(x => (Random.nextInt(10).toString, 1L))
          .keyBy(x => x._1)
          .window(TumblingEventTimeWindows.of(Time.seconds(1)))
          .aggregate(new AggregateFunction[(String, Long), Long, Long] {
              override def createAccumulator(): Long = 0L

              override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1L

              override def getResult(accumulator: Long): Long = accumulator

              override def merge(a: Long, b: Long): Long = a + b
          }, new WindowFunction[Long, PVCount, String, TimeWindow] {

              override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PVCount]): Unit = {
                  out.collect(PVCount(key, new Timestamp(window.getEnd).toString, input.head))
              }
          }).map(x => ("pv", x.cnts))
          // 合
          .keyBy(x => x._1)
          .sum(1)
          .print()


        env.execute(this.getClass.getSimpleName)
    }
}