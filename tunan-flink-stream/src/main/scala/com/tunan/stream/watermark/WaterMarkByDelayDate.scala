package com.tunan.stream.watermark

import java.sql.Timestamp

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMarkByDelayDate {
    // 定于允许的最大乱序时间
    val MAX_ALLOWED_UNBOUNDED_TIME: Long = 1000*10L

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 这里并行度的设置很重要，不然数据不在一个分区内
        env.setParallelism(1)
        // 设置EventTime作为window时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val output = new OutputTag[Info]("late")

        // 窗口划分: [0,3)，[3,6)，[6,9) ...
        val lines = env.socketTextStream("aliyun", 9999)
        val stream = lines.map(row => {
            val words = row.split(",").map(_.trim)
            Info(words(0), words(1).toLong, words(2).toDouble)
            // 指定watermark允许的乱序时间，时间上是窗口触发操作向后推迟，如果watermark time > window end time就触发，还没有来的数据默认都丢弃
        }).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(MAX_ALLOWED_UNBOUNDED_TIME))
          .keyBy(0)
          // 滚动窗口大小为3秒
          .window(TumblingEventTimeWindows.of(Time.seconds(3)))
          // 允许窗口等待时间 (不太懂)
          .allowedLateness(Time.seconds(2))
          // 存储延迟数据
          .sideOutputLateData(output)
          .apply(new RichWindowFunction[Info, String, Tuple, TimeWindow] {
              override def apply(key: Tuple, window: TimeWindow, input: Iterable[Info], out: Collector[String]): Unit = {
                  val totalSize = input.size
                  var totalScore = 0.0D
                  for (ele <- input) {
                      totalScore += ele.Score
                  }
                  val avg = totalScore / totalSize.toDouble

                  out.collect(s"结果: ${avg}, 窗口的开始时间: ${new Timestamp(window.getStart)}, 窗口的结束时间: ${new Timestamp(window.getEnd)}")
              }
          })

        stream.print()
        // 拿到延迟数据
        stream.getSideOutput(output).print("延迟数据: ")


        env.execute(this.getClass.getSimpleName)
    }
}


class MyAssignerWithPeriodicWatermarks(maxAllowedUnorderedTime: Long) extends AssignerWithPeriodicWatermarks[Info] {
    // 定义变量，保存的是最大的时间戳，这个时间戳只会更新更大的数值
    var maxTimestamp = 0L
    override def getCurrentWatermark: Watermark = {
        // WaterMark Time
        new Watermark(maxTimestamp - maxAllowedUnorderedTime)
    }

    // TODO 提取时间戳
    override def extractTimestamp(element: Info, previousElementTimestamp: Long): Long = {
        // 进来的时间是秒，这里转成了毫秒
        val currentTime = element.Time * 1000
        // 拿到最大的时间戳返回去
        maxTimestamp = maxTimestamp.max(currentTime)

        println(s"数值: ${element.Score}, 当前时间搓: ${new Timestamp(currentTime)}, 最大时间戳: ${new Timestamp(maxTimestamp)} , watermark时间戳: ${new Timestamp(getCurrentWatermark.getTimestamp)}")
        currentTime

    }
}

