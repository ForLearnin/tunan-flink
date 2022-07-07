package com.tunan.stream.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


object ScoreAcc {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 启动checkpoint
    env.enableCheckpointing(1000)
    // 精准一次的模式
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 默认就是这个
    // 保留所有检查点状态
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置StateBackend
    env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoint"))
    // 设置失败重试机制
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(2)))

    val stream = env.socketTextStream("aliyun", 9999)

    stream.filter(_.nonEmpty).map(row => {
      (row.split(",")(0),row.split(",")(1).toInt)
    }).keyBy(x => x._1)
      .flatMap(new RichFlatMapFunction[(String,Int),(String,Int)] {

        // 定义一个状态，分别记录次数和数值
        private var sumState: ValueState[Int] = _


        override def open(parameters: Configuration): Unit = {
          // 上下文拿到状态
          val valueStateDescriptor = new ValueStateDescriptor("sumState", classOf[Int])
          sumState = getRuntimeContext.getState(valueStateDescriptor)
        }

        override def flatMap(in: (String, Int), collector: Collector[(String, Int)]): Unit = {
          // 拿到状态的值
          val tmpState = sumState.value()

          // 初始化状态值
          val currentState:Int = if (null != tmpState) {
            tmpState
          } else {
            0
          }

          // 累加次数

          val newState = currentState + in._2

          // 更新状态
          sumState.update(newState)

          // 满足条件输出
          collector.collect(in._1,newState)

        }
      }).print()

    env.execute(this.getClass.getSimpleName)
  }
}