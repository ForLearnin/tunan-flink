package com.tunan.stream.sink

import com.tunan.stream.bean.Access
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


object WriteToRedis {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val files = env.readTextFile("tunan-flink-stream/data/access.txt")

		val result = files.map(row => {
			val words = row.split(",").map(_.trim)
			Access(words(0).toLong, words(1), words(2).toLong)
		}).keyBy(x => (x.time, x.domain)).sum(2)

		class CustomRedis extends RedisMapper[Access]{
			override def getCommandDescription: RedisCommandDescription = {
				new RedisCommandDescription(RedisCommand.HSET, "access")
			}

			override def getKeyFromData(data: Access): String = data.domain

			override def getValueFromData(data: Access): String = data.traffics.toString
		}

		val conf = new FlinkJedisPoolConfig
		.Builder()
		  .setHost("aliyun")
		  .setPort(16379)
		  .setTimeout(60000)
		  .build()
		result.addSink(new RedisSink[Access](conf, new CustomRedis))

		env.execute(this.getClass.getSimpleName)
	}
}