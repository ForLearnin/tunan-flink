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

object WMApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val output = new OutputTag[Info2]("late")

    val MAX_ALLOWED_UNBOUNDED_TIME = 10000L
    val stream = env.socketTextStream("aliyun", 9999)
      .map(x => {
        val splits = x.split(",")
        Info2(splits(0).trim, splits(1).trim.toDouble, splits(2).trim, splits(3).trim.toLong)
      }).assignTimestampsAndWatermarks(new RuozedataAssignerWithPeriodicWatermarks(MAX_ALLOWED_UNBOUNDED_TIME))
        .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .allowedLateness(Time.seconds(2)) // 允许等待两秒
      .sideOutputLateData(output)
      .apply(new RichWindowFunction[Info2,String,Tuple,TimeWindow] {
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[Info2], out: Collector[String]): Unit = {

            val totals = input.size
            var totalTmp = 0.0
            input.foreach(x => totalTmp = totalTmp + x.temperature)
            val avg = totalTmp / totals

            out.collect(s"$avg, ${new Timestamp(window.getStart)}, ${new Timestamp(window.getEnd)}")
          }
        })

      stream.print()
    stream.getSideOutput(output).print("-----")

    env.execute(this.getClass.getSimpleName)

  }
}

class RuozedataAssignerWithPeriodicWatermarks(maxAllowedUnorderedTime: Long) extends AssignerWithPeriodicWatermarks[Info2] {

  var maxTimestamp: Long = 0

  override def extractTimestamp(element: Info2, previousElementTimestamp: Long): Long = {
    val nowTime = element.time * 1000
    maxTimestamp = maxTimestamp.max(nowTime)

    println(new Timestamp(nowTime) + "," + new Timestamp(maxTimestamp) + "," + new Timestamp(getCurrentWatermark.getTimestamp))
    nowTime
  }

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTimestamp - maxAllowedUnorderedTime)
  }
}

case class Info2(id: String, temperature: Double, name: String, time: Long)