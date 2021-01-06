package com.tunan.kafka.stream

import java.util.Properties

import com.tunan.kafka.bean.Access
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.api.scala._

object FlinkKafkaIO {

    val BOOTSTRAP_SERVERS = "aliyun:9092"
    val GROUP_ID = "kafka-flink-input_group"
    val TOPIC = "kafka-flink-input"



    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//        env.addSource(kafkaConsumer()).setParallelism(2).filter(x => x.trim.nonEmpty)
//          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
//              override def extractTimestamp(element: String): Long = {
//                  // 这里返回的时间必须是毫秒
//                  element.split(",")(1).toLong
//              }
//          })
//          .map(x => {
//              val splits = x.split(",").map(_.trim)
//              (splits(0),splits(2).toInt)
//          })
//          .keyBy(_._1)
//          .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//          .sum(1)
//          .print()


        kafkaProducer(env)

        env.execute(this.getClass.getSimpleName)
    }

    private def kafkaConsumer(): FlinkKafkaConsumer[String] = {
        val properties = new Properties
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS)
        properties.setProperty("group.id", GROUP_ID)

        new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema(), properties)
    }

    private def kafkaProducer(env: StreamExecutionEnvironment) = {
            val stream = env.fromCollection(List(
              Access(202010080010L, "ruozedata.com", 2000).toString,
              Access(202010080010L, "ruoze.ke.qq.com", 6000).toString,
              Access(202010080010L, "google.com", 5000).toString,
              Access(202010080010L, "ruozedata.com", 4000).toString,
              Access(202010080010L, "ruoze.ke.qq.com", 1000).toString
            ))
        val properties = new Properties
        properties.put("bootstrap.servers", BOOTSTRAP_SERVERS)
            // TODO... 数据清洗、数据分类...
            val producer = new FlinkKafkaProducer[String](BOOTSTRAP_SERVERS,TOPIC, new SimpleStringSchema())

            stream.addSink(producer)

            stream.print()
    }


}
