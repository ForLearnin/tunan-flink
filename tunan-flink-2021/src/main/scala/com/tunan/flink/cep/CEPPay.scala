package com.tunan.flink.cep

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object CEPPay {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.setParallelism(1)

		val elements = env.fromElements(
			"111111,pay,1622689955",
			"111111,create,1622689955",
			"222222,create,1622689955",
			"222222,pay,1622689955"
		)

		val input = elements.map(row => {
			val splits = row.split(",")
			Pay(splits(0), splits(1), splits(2).toLong)
		}).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Pay](Time.seconds(0)) {
			override def extractTimestamp(t: Pay): Long = t.time * 1000
		}).keyBy(_.user)

		val pattern = Pattern.begin[Pay]("first").where(_.act.equalsIgnoreCase("create"))
		  .followedBy("next").where(_.act.equalsIgnoreCase("pay"))
		  .within(Time.seconds(10))

		val stream = CEP.pattern(input, pattern)

		// 通知
		val output = new OutputTag[Notice]("notice")

		val result = stream.select(output, new PatternTimeoutFunction[Pay, Notice] {
			override def timeout(map: util.Map[String, util.List[Pay]], l: Long): Notice = {
				val first = map.get("first").get(0)
				Notice(first.user, first.time, 0, "支付失败。。。")
			}
		}, new PatternSelectFunction[Pay, Notice] {
			override def select(map: util.Map[String, util.List[Pay]]): Notice = {
				val first = map.get("first").get(0)
				val next = map.get("next").get(0)
				Notice(first.user, first.time, next.time, "支付成功。。。")
			}
		})

		result.print()
		result.getSideOutput(output).print()


		env.execute(this.getClass.getSimpleName)
	}
	case class Pay(user:String,act:String,time:Long)
	case class Notice(user:String,first:Long,next:Long,context:String)
}
