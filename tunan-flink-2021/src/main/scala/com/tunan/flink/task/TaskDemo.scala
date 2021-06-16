package com.tunan.flink.task

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TaskDemo {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(4)

		val stream = env.socketTextStream("aliyun", 9999)

		stream
		  .flatMap(_.split(","))
		  .map((_, 1))
		  .keyBy(0)
		  .sum(1)
		  .print()
  		  .setParallelism(2)


		env.execute(this.getClass.getSimpleName)
	}
}