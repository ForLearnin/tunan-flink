package com.tunan.kafka.stream

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FliinkKafkaStateManager {
    val BOOTSTRAP_SERVERS = "aliyun:9092"
    val GROUP_ID = "kafka-flink-input_group"
    val TOPIC = "kafka-flink-input"

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hadoop")

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.enableCheckpointing(5000)
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoint"))
//        env.setStateBackend(new FsStateBackend("file:///sparkspace/tunan-flink/tunan-flink-kafka/checkpoint"))
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000))

        val properties = new Properties
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS)
        properties.setProperty("group.id", GROUP_ID)
        properties.setProperty("auto.offset.reset", "earliest")
        properties.setProperty("enable.auto.commit", "false")

        val consumer = new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema, properties)
        env.addSource(consumer)
          .flatMap(_.split(","))
          .map((_, 1))
          .keyBy(0)
          .sum(1)
          .print()


        env.execute(this.getClass.getSimpleName)
    }
}
