package com.tunan.stream.state

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * @Auther: 李沅芮
 * @Date: 2021/11/30 13:48
 * @Description: ByKey行为流匹配规则
 */
object KeyedBroadcastMapState {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 行为流
        val actions: DataStream[Action] = env.socketTextStream("hadoop1", 9998).map(row => {
            val lines = row.split(",").map(_.trim)
            Action(lines(0).toInt, lines(1))
        })

        // 匹配模式流
        val patterns: DataStream[Pattern] = env.socketTextStream("hadoop1", 9999).map(row => {
            val lines = row.split(",").map(_.trim)
            Pattern(lines(0), lines(1))
        })

        // 用户id分区
        val actionsByUser = actions.keyBy(_.userId)

        // 定义一个MapState描述，给下面的方法传参
        val bcStateDescriptor = new MapStateDescriptor("patterns", classOf[Null], classOf[Pattern])

        // 普通流转成广播流
        val broadcastPatterns = patterns.broadcast(bcStateDescriptor)

        // 行为流匹配广播流
        actionsByUser.connect(broadcastPatterns).process(new KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)] {

            // 定义每个用户的上一次行为
            var prevActionState: ValueState[String] = _
            // 定义匹配状态模式
            var patternDesc: MapStateDescriptor[Null, Pattern] = _

            // 可以实现定时器，这里暂时没用到
            override def onTimer(timestamp: Long, ctx: KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)]#OnTimerContext, out: Collector[(Long, Pattern)]): Unit = super.onTimer(timestamp, ctx, out)

            // 初始化状态
            override def open(parameters: Configuration): Unit = {
                prevActionState = getRuntimeContext.getState(new ValueStateDescriptor[String]("lastAction", classOf[String]))
                patternDesc = new MapStateDescriptor("patterns", classOf[Null], classOf[Pattern])
            }

            // TODO 处理行为流
            override def processElement(value: Action, ctx: KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)]#ReadOnlyContext, out: Collector[(Long, Pattern)]): Unit = {
                // 从状态中得到已存储的匹配模式
                val pattern = ctx.getBroadcastState(patternDesc).get(null)
                // 拿到该用户上一次的行为
                val prevAction = prevActionState.value()
                // 判断该用户的上一次行为是否符合匹配规则，以及当前的行为是否符合匹配规则
                if (null != pattern && null != prevAction) {
                    if (pattern.firstAction.equals(prevAction) &&
                        pattern.secondAction.equals(value.action)) {
                        out.collect((ctx.getCurrentKey, pattern))
                    }
                }
                // 把当前的行为更新为上一次行为
                prevActionState.update(value.action)
            }

            // 匹配模式流
            override def processBroadcastElement(value: Pattern, ctx: KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)]#Context, out: Collector[(Long, Pattern)]): Unit = {
                // 如果有新的模式进来了，就拿到原来的状态，更新为最新的匹配模式
                val bcState: BroadcastState[Null, Pattern] = ctx.getBroadcastState(patternDesc)
                bcState.put(null, value)
            }
        }).print()

        env.execute(this.getClass.getSimpleName)
    }
}


case class Action(userId: Long, action: String)

case class Pattern(firstAction: String, secondAction: String)