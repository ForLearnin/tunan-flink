package com.tunan.stream.timer

import com.tunan.stream.bean.{Behavior, ItemCount}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 *
 * 需求
 * 1. 每1分钟统计10分钟商品条目下购买行为的次数
 * 2. 在需求1的前提下统计行为的次数的降序排序和TopN
 */

object HotRankAndTopN {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        env.setParallelism(1)

        val stream = env.socketTextStream("aliyun", 9999)
          .filter(row => {
              var flag = true
              if (null == row || "" == row) {
                  flag = false
              }

              if (row.split(",").length != 5) {
                  flag = false
              }

              flag
          })
          .map(row => {
              val splits = row.split(",")

              Behavior(splits(0), splits(1), splits(2), splits(3), format.parse(splits(4)).getTime)
          })
          //          .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(0))
          //            .withTimestampAssigner(new SerializableTimestampAssigner[Behavior] {
          //                override def extractTimestamp(element: Behavior, recordTimestamp: Long): Long = element.timestamp
          //            }))
          .keyBy(x => (x.itemId, x.behavior))
          .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
          .aggregate(new AggregateFunction[Behavior, Long, Long] {
              override def createAccumulator(): Long = 0L

              override def add(value: Behavior, accumulator: Long): Long = accumulator + 1L

              override def getResult(accumulator: Long): Long = accumulator

              override def merge(a: Long, b: Long): Long = ???
          }, new WindowFunction[Long, ItemCount, (String, String), TimeWindow] {
              override def apply(key: (String, String), window: TimeWindow, input: Iterable[Long], out: Collector[ItemCount]): Unit = {
                  out.collect(ItemCount(key._1, key._2, window.getStart, window.getEnd, input.head))
              }
          })

        // 按照用户行为的次数做排序和topN
        stream.keyBy(x => (x.behavior, x.start, x.end))
          .process(new KeyedProcessFunction[(String, Long, Long), ItemCount, String] {
              private var valueState: ValueState[ListBuffer[ItemCount]] = _
              val desc = new ValueStateDescriptor[ListBuffer[ItemCount]]("vs", classOf[ListBuffer[ItemCount]])

              override def open(parameters: Configuration): Unit = {
                  valueState = getRuntimeContext.getState(desc)
              }

              // 处理每条数据
              override def processElement(value: ItemCount, ctx: KeyedProcessFunction[(String, Long, Long), ItemCount, String]#Context, out: Collector[String]): Unit = {
                  var buffer = valueState.value()
                  if (null == buffer) {
                      buffer = ListBuffer[ItemCount]()
                  }

                  buffer += value
                  valueState.update(buffer)

                  // 窗口关闭的时间出发定时器
                  ctx.timerService().registerEventTimeTimer(value.end + 1)

              }

              // 执行定时器
              override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(String, Long, Long), ItemCount, String]#OnTimerContext, out: Collector[String]): Unit = {
                  val buffer = valueState.value()

                  // 自定义排序器 用来替换下面的那种性能低的方法
                  val ord: Ordering[ItemCount] = new Ordering[ItemCount]() {
                      override def compare(x: ItemCount, y: ItemCount): Int = {
                          if (!x.behavior.equals(y.behavior) && x.count == y.count) {
                              return 1
                          }

                          (y.count - x.count).toInt
                      }
                  }

                  // 使用Set集合，高性能
                  val set = mutable.TreeSet.empty(ord)
                  buffer.foreach(x => {
                      set.add(x)
                      if (set.size > 1) {
                          set.remove(set.last)
                      }
                  })

                  // 数据量大的时候性能不好
                  //                  val sortBuffer = buffer.sortBy(- _.count).take(1)
                  valueState.clear()
                  // 写出
                  out.collect(set.mkString(" "))
              }
          })
          .print()


        env.execute(this.getClass.getSimpleName)

    }
}
