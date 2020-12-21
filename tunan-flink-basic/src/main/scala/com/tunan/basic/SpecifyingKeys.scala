package com.tunan.basic

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SpecifyingKeys {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		val lines = env.socketTextStream("aliyun", 9999)
		lines
		  .flatMap(_.toLowerCase().split(","))
		  .filter(_.nonEmpty)
		  .map((_, 1))
		  .keyBy(_._1)
		  .sum(1)
		  .print()

		//		Batch(env)
		env.execute(getClass.getSimpleName)
	}

	private def Batch(env: ExecutionEnvironment) = {
		val file = env.readTextFile("tunan-flink-basic/data/word.txt")
		file
		  .flatMap(_.toLowerCase.split(","))
		  .filter(_.nonEmpty)
		  .map(User(_, 1))
		  .groupBy(0)
		  //		  		  .groupBy(_.name)
		  //		  .groupBy("name")
		  .sum(1).print()
	}
}

case class User(name: String, cnt: Int)
