package com.tunan.stream.transformation

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
object Partitioner {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val socket = env.socketTextStream("aliyun", 9999)
		socket.map(row => {
			val words = row.split(",")
			(words(0),words(1).toInt)
		}).partitionCustom(new MyPartitioner ,_._1)


		env.execute(this.getClass.getSimpleName)
	}
}


class MyPartitioner extends Partitioner[String] {

	override def partition(key: String, numPartitions: Int): Int = {
		key match {
			case "zs" => 1
			case "ls" => 2
			case "ww" => 3
			case _ => 4
		}
	}
}
