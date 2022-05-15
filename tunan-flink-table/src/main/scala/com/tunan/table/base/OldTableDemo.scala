package com.tunan.table.base

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._


import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
object OldTableDemo {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		env.setParallelism(1)

		val socketStream = env.socketTextStream("aliyun", 9999)

		socketStream
		  .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
			.withTimestampAssigner(new SerializableTimestampAssigner[String] {
				override def extractTimestamp(t: String, l: Long): Long = t.split(",")(2).toLong
			}))

		val mapStream = socketStream.map(row => {
			val words = row.split(",")
			Event(words(0), words(1), words(2).toLong)
		})

		// 创建表执行环节
		val tableEnv = StreamTableEnvironment.create(env)

		// 将DataStream转成Tanle
		val eventTable = tableEnv.fromDataStream(mapStream)

		// 基于sql进行转换
		val resultTable1 = tableEnv.sqlQuery(s"SELECT name,behavior,ts from $eventTable")

		// 基于Table进行转换
		val resultTable2 = eventTable.select($("name"), $("behavior"), $("ts"))
		  .where($("name").isEqual(${
			  "zs"
		  }))

		// 转换成流打印输出(不更新)
		tableEnv.toDataStream(resultTable1).print("result1")
		tableEnv.toDataStream(resultTable2).print("result2")

		// 聚合查询
		val aggResultTable = tableEnv.sqlQuery("select name,count(1) from a")
		// 转换成流打印输出(可更新)
		tableEnv.toChangelogStream(aggResultTable).print("agg")

		env.execute(this.getClass.getSimpleName)
	}

	case class Event(name:String,behavior:String,ts:Long)
}
