package com.tunan.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object CountTrigger {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val stream = env.socketTextStream("aliyun", 9999).filter(_.trim.nonEmpty)

        // CountTrigger单单一个触发器只能触发一种条件，通过自定义触发器可以同时满足多种条件时触发
        stream
          .map(x => (x, 1))
          .keyBy(0)
          // 基于时间和数量两个条件触发
          .timeWindow(Time.seconds(5))
          //          .trigger(org.apache.flink.streaming.api.windowing.triggers.CountTrigger.of(5))
          .trigger(CountTrigger.of(5))
          .sum(1)
          .print()


        env.execute(this.getClass.getSimpleName)
    }


    def of(max: Int): CountTrigger = {
        new CountTrigger(max)
    }
}

class CountTrigger(max: Int) extends Trigger[(String, Int), TimeWindow] {

    // 定义StateDescriptor
    private val stateDesc = new ReducingStateDescriptor[Int]("count", new ReduceFunction[Int] {
        override def reduce(value1: Int, value2: Int): Int = {
            value1 + value2
        }
    }, classOf[Int])

    // 作用每个元素
    override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        ctx.registerProcessingTimeTimer(window.maxTimestamp())
        val state = ctx.getPartitionedState(stateDesc)
        state.add(1)

        val value = state.get()
        if (value >= max) {
            state.clear()
            TriggerResult.FIRE_AND_PURGE
        } else {
            TriggerResult.CONTINUE
        }
    }

    // 处理时间触发
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.FIRE_AND_PURGE
    }

    // 事件时间触发
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
        TriggerResult.CONTINUE
    }

    // 清空
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
        ctx.getPartitionedState(stateDesc).clear()
        ctx.deleteProcessingTimeTimer(window.maxTimestamp())
    }

    // 是否合并
    override def canMerge: Boolean = true

    // 合并
    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
        ctx.mergePartitionedState(stateDesc)
    }
}
