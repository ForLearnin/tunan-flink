package com.tunan.kafka.sink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import redis.clients.jedis.Jedis

class RedisSink extends RichSinkFunction[(String, String, Int)] {

    private var jedis: Jedis = _

    override def invoke(value: (String, String, Int)): Unit = {
        if (!jedis.isConnected) {
            jedis.connect()
        }
        jedis.hset(value._1, value._2, value._3 + "")
    }

    override def open(parameters: Configuration): Unit = {
        val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
        val host = parameters.getRequired("redis.host")
        val port = parameters.getInt("redis.port", 6379)
        val db = parameters.getInt("redis.db", 0)

        jedis = new Jedis(host, port, 6000)
        println("打开连接: " + jedis)
        jedis.select(db)
    }

    override def close(): Unit = {
        if (null != jedis) {
            println(s"关闭连接: ${jedis}")
            jedis == null
        }
    }
}
