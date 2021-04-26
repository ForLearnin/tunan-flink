package com.tunan.flink.hbase

import org.apache.flink.addons.hbase.{HBaseTableSchema, HBaseUpsertSinkFunction, HBaseWriteOptions}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.flink.streaming.api.scala._

import java.util.Properties

object HBaseStreamSinkApp {


    def write2HBaseWithRichSinkFunction(): Unit ={
        val topic = "test"
        val props = new Properties
        props.put("bootstrap.servers", "aliyun:9092")
        props.put("group.id", "test")
        props.put("enable.auto.commit", "true")
        props.put("auto.commit.interval.ms", "1000")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.enableCheckpointing(3000)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, props)
        val stream = env.addSource(consumer)
        stream.addSink(new HBaseWriter)

        env.execute(this.getClass.getSimpleName)
    }


    def main(args: Array[String]): Unit = {

        write2HBaseWithRichSinkFunction()

    }

    class HBaseWriter extends RichSinkFunction[String]{
        var conn:Connection = _
        val scan:Scan = null
        var mutator:BufferedMutator = _
        var count:Long = 1


        override def open(parameters: Configuration): Unit = {
            val config = HBaseConfiguration.create()
            config.set(HConstants.ZOOKEEPER_QUORUM, "aliyun")
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
            config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
            config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
            conn = ConnectionFactory.createConnection(config)

            val options = new HBaseWriteOptions(10 * 1024, 2, 1000)
            options


            val table = TableName.valueOf("student")
            val params = new BufferedMutatorParams(table)
            params.writeBufferSize(1024 * 1024)
            mutator = conn.getBufferedMutator(params)
            count = 1
        }

        override def invoke(in: String): Unit = {
            val cf = "cf"
            val words = in.split(",")

            println(words.mkString(" - "))
            val put = new Put(Bytes.toBytes(words(0)))
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("name"),Bytes.toBytes(words(1)))
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("age"),Bytes.toBytes(words(2)))
            mutator.mutate(put)

            // 每满2000条刷一次数据
            if(count >= 2){
                mutator.flush()
                count = 0
            }

            println(s"数据量: ${count}")

            count += 1
        }


        override def close(): Unit = {
            if(conn != null){
                conn.close()
            }
        }
    }

    class newHBaseWrite {

        val config = HBaseConfiguration.create()
        config.set(HConstants.ZOOKEEPER_QUORUM, "aliyun")
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

        private val schema = new HBaseTableSchema()
        schema.setRowKey("aaaa",classOf[String])
        schema.addColumn("cf","name",classOf[String])
        schema.addColumn("cf","age",classOf[String])

        private val hbaseSin: HBaseUpsertSinkFunction = new HBaseUpsertSinkFunction("student",schema,config,100*1024,2,1000)


    }
}