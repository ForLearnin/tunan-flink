package com.tunan.utils

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FlinkKafkaSource {

    private val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    def getEnv: StreamExecutionEnvironment = env

    def createKafkaSource(parameters: ParameterTool): DataStream[String] = {
        env.getConfig.setGlobalJobParameters(parameters)
        // kafka保证幂等性的配置
//        env.enableCheckpointing(parameters.getLong("checkpoint.interval", 2000L))
//        env.setStateBackend(new FsStateBackend(parameters.getRequired("checkpoint.path")))
//        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(parameters.getInt("restart.count", 3), parameters.getInt("restart.time", 2000)))

        val properties = new Properties()
        properties.put("bootstrap.servers", parameters.getRequired("brokers"))
        properties.put("group.id", parameters.getRequired("group.id"))
        properties.put("auto.offset.reset", parameters.get("auto.offset.reset", "earliest"))
        properties.put("enable.auto.commit", parameters.get("enable.auto.commit", "false"))

        import scala.collection.JavaConversions._
        val topics = parameters.getRequired("topics")
        val consumer = new FlinkKafkaConsumer[String](topics.split(",").toList, new SimpleStringSchema(), properties)
        env.addSource(consumer)

    }
}
