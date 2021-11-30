package com.tunan.stream.state

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit

/**
 * @Auther: 李沅芮
 * @Date: 2021/11/30 08:55
 * @Description:  广播单个值
 */
object BroadcastValue {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 配置处理环境的并发度为4
        env.setParallelism(4)

        val configKeyWordsDesc = new MapStateDescriptor[String, String]("config-keywords", classOf[String], classOf[String])

        // 自定义广播流（单例）
        val broadcastStream: BroadcastStream[String] = env.addSource(new RichSourceFunction[String] {

            private var isRunning = true

            //测试数据集
            val dataSet = Array[String](
                "java",
                "swift",
                "php",
                "go",
                "python"
            )

            /**
             * 数据源：模拟每30秒随机更新一次拦截的关键字
             *
             * @param ctx
             * @throws Exception
             */
            override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
                val size = dataSet.length
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(30)
                    val seed: Int = (Math.random() * size).toInt
                    ctx.collect(dataSet(seed))
                    println("读取到上游发送的关键字:" + dataSet(seed))
                }
            }

            override def cancel(): Unit =
                isRunning = false
        }).setParallelism(1).broadcast(configKeyWordsDesc)

        // 自定义数据流（单例）
        val dataStream  = env.addSource(new RichSourceFunction[String] {

            private var isRunning = true

            //测试数据集
            private val dataSet = Array[String](
                "java是世界上最优秀的语言",
                "swift是世界上最优秀的语言",
                "php是世界上最优秀的语言",
                "go是世界上最优秀的语言",
                "python是世界上最优秀的语言")

            /**
             * 模拟每3秒随机产生1条消息
             *
             * @param ctx
             * @throws Exception
             */
            override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
                val size = dataSet.length
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(3)
                    val seed = (Math.random() * size).toInt
                    ctx.collect(dataSet(seed))
                    println("读取到上游发送的消息：" + dataSet(seed))
                }
            }

            override def cancel(): Unit = {
                isRunning = false
            }
        }).setParallelism(1)

        dataStream.connect(broadcastStream).process(new BroadcastProcessFunction[String,String,String] {

            // 拦截的关键字
            private var keyWords:String = _

            override def open(parameters: Configuration): Unit = {
                keyWords = "java"
                println("初始化模拟连接数据库读取拦截关键字：java")
            }

            // 正常数据流
            override def processElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
                if(value.contains(keyWords)){
                    out.collect("拦截消息:" + value + ", 原因:包含拦截关键字：" + keyWords)
                }
            }

            // 状态流
            override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
                keyWords = value
                println("关键字更新成功，更新拦截关键字：" + value)
            }
        }).print

        env.execute(this.getClass.getSimpleName)
    }
}
