package com.tunan.stream.timer

import java.sql.Timestamp

import com.tunan.stream.bean.{AccessPage, CountByProvince}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TimerProcess {

    val outputTag = new OutputTag[String]("warn")

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)
        val accessPage = env.readTextFile("tunan-flink-stream/data/log.txt")
          .map(row => {
              val splits = row.split(",").map(_.trim)
              AccessPage(splits(0), splits(1), splits(2), splits(3).toLong)
          }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[AccessPage](Time.seconds(0)) {
            override def extractTimestamp(element: AccessPage): Long = element.ts
        })

        // 定时器
        val timerCount = accessPage.keyBy(x => (x.userId.trim, x.domain.trim, x.province.trim))
          .process(new FilterProcess(40))

        // 统计用户、域名、省份对应的数量
        timerCount.keyBy(x => (x.userId, x.domain, x.province))
          // 每10分钟 求一个小时的统计 ， 不需要保存状态
          .timeWindow(Time.hours(1), Time.minutes(10))
          .aggregate(new AggregateFunction[AccessPage, Long, Long] {
              override def createAccumulator(): Long = 0L

              override def add(value: AccessPage, accumulator: Long): Long = accumulator + 1L

              override def getResult(accumulator: Long): Long = accumulator

              override def merge(a: Long, b: Long): Long = ???
          }, new WindowFunction[Long, CountByProvince, (String, String, String), TimeWindow] {
              override def apply(key: (String, String, String), window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
                  out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key._1 + "_" + key._2 + "_" + key._3, input.iterator.next))
              }
          }).print

        timerCount.getSideOutput(outputTag).print("===")
        env.execute(this.getClass.getSimpleName)
    }


    class FilterProcess(max: Int) extends KeyedProcessFunction[(String, String, String), AccessPage, AccessPage] {

        // 定义状态
        var countState: ValueState[Long] = _
        var sentState: ValueState[Boolean] = _
        var timerState: ValueState[Long] = _


        override def open(parameters: Configuration): Unit = {
            // 初始化状态
            countState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
            sentState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("set", classOf[Boolean]))
            timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, String, String), AccessPage, AccessPage]#OnTimerContext, out: Collector[AccessPage]): Unit = {
            // 每天都从0开始计数
            if (timestamp == timerState.value()) {
                sentState.clear()
                countState.clear()
                timerState.clear()
            }
        }

        override def processElement(value: AccessPage, ctx: KeyedProcessFunction[(String, String, String), AccessPage, AccessPage]#Context, out: Collector[AccessPage]): Unit = {
            // 拿到当前key的count数量
            val currentCount = countState.value()

            // 第一次进入，触发定时器
            if (currentCount == 0) {
                // 拿到明天0点的时间
                val time = ((ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24)) + 1) * (1000 * 60 * 60 * 24) - (1000 * 60 * 60 * 8)
                // 更新触发器状态中的时间
                timerState.update(time)
                // 注册明天0点的时间到定时器中准备触发
                ctx.timerService().registerEventTimeTimer(time)
            }

            // 如果超过阈值，触发警报，但是只触发一次
            if (currentCount >= max) {
                if (!sentState.value()) {
                    sentState.update(true)
                    ctx.output(outputTag, value.userId + "," + value.domain + new Timestamp(value.ts) + "超过阈值:" + max)
                }
                // 如果没有超过阈值则继续输出，并且更新count的状态
            } else {
                countState.update(currentCount + 1)
                out.collect(value)
            }
        }
    }

}


