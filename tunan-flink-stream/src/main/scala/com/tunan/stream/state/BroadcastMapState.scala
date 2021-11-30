package com.tunan.stream.state

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * @Auther: 李沅芮
 * @Date: 2021/11/30 09:43
 * @Description: 广播MapState
 *
 * 三种应用场景
        动态配置更新
        规则改变
        类似开关的功能
        假设场景，
        有两条流，一条是普通的流，另一条是控制流，如果需要动态调整代码逻辑时，可以使用广播状态
 */
object BroadcastMapState {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("hadoop1", 9999)
        val propStream = env.socketTextStream("hadoop1", 9998)

        //TODO 1.把其中一条流(控制流) 广播出去
        //定义一个Map状态描述器,控制流会把这个状态广播出去
        val broadcastDesc = new MapStateDescriptor[String, String]("broadcast-state", classOf[String], classOf[String])
        val broadcastStream = propStream.broadcast(broadcastDesc)

        //TODO 2.把另一条流和广播流关联起来
        val connectStream = inputStream.connect(broadcastStream)

        //TODO 3.调用Process
        connectStream.process(new BroadcastProcessFunction[String,String,String] {
            /*
                获取广播状态，获取数据进行处理
             */
            override def processElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {

                //TODO 5.通过上下文获取广播状态，取出里面的值
                val broadcastState = ctx.getBroadcastState(broadcastDesc)
                val switch = broadcastState.get("switch")
                if("1".equals(switch)){
                    out.collect("切换到1的逻辑")
                }else if("2".equals(switch)){
                    out.collect("切换到2的逻辑")
                }else{
                    out.collect("找不到切换的逻辑")
                }
            }
            /**
             * 处理广播流的数据：这里主要定义，什么数据往广播状态存
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
                val broadcastState = ctx.getBroadcastState(broadcastDesc)
                broadcastState.put("switch",value)
            }
        }).print()

        env.execute(this.getClass.getSimpleName)
    }
}
