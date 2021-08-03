package com.tunan.flink.hbase

import org.apache.flink.addons.hbase.{HBaseTableSchema, HBaseUpsertSinkFunction, HBaseWriteOptions}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, BufferedMutatorParams, Connection, ConnectionFactory, Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.apache.hadoop.conf.Configuration

import java.util.Properties

object HBaseStreamSinkApp {


    def write2HBaseWithRichSinkFunction(): Unit ={
        val topic = "test22"
        val props = new Properties
        props.put("bootstrap.servers", "aliyun:9092")
        props.put("group.id", "test")
        props.put("enable.auto.commit", "false")
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
        stream.addSink(new newHBaseWrite)

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


        override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
            val config = HBaseConfiguration.create()
            config.set(HConstants.ZOOKEEPER_QUORUM, "aliyun")
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
            config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
            config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
            conn = ConnectionFactory.createConnection(config)


            val table = TableName.valueOf("student")
            val params = new BufferedMutatorParams(table)
            params.writeBufferSize(1024 * 1024)
            mutator = conn.getBufferedMutator(params)
            count = 1
        }

        override def invoke(in: String): Unit = {
            val cf = "cf"
            val words = in.split(",")


            if(words.length == 3){
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
        }


        override def close(): Unit = {
            if(conn != null){
                conn.close()
            }
        }
    }

    class newHBaseWrite extends RichSinkFunction[String]{

        var hbaseSink: HBaseUpsertSinkFunction = _
        var config:Configuration = _

        override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
            config = HBaseConfiguration.create()
            config.set(HConstants.ZOOKEEPER_QUORUM, "aliyun")
            config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
            config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
            config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

            val schema = new HBaseTableSchema()
            schema.setRowKey("id",classOf[String])
            schema.addColumn("cf","name",classOf[String])
            schema.addColumn("cf","age",classOf[String])


            hbaseSink = new HBaseUpsertSinkFunction("student",schema,config,Int.MaxValue,3,Int.MaxValue)
            hbaseSink.open(null)
        }

        override def invoke(in: String): Unit = {

            val words = in.split(",")


            if(words.length == 3){
                println(words.mkString(" - "))
                val row = new Row(2)
                val f = new Row(2)
                // f: 放两个值，分别是name,age
                f.setField(0,words(1))
                f.setField(1,words(2))
                // row: 放两个值，分别是key,f
                row.setField(0,words(0))
                row.setField(1,f)

                hbaseSink.invoke(new org.apache.flink.api.java.tuple.Tuple2(true,row),null)

            }
        }

        override def close(): Unit = {
            if(hbaseSink != null){
                hbaseSink.close()
            }
        }
    }
}