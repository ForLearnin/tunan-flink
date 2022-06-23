package com.tunan.flink.cep

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time


object CEPFailure {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		val stream = env.fromElements(
			"001,27.49.10.1,success,1622689918",
			"002,27.49.9.16,failure,1622689952",
			"002,27.49.9.16,failure,1622689953",
			"002,27.49.9.16,failure,1622689954",
			"002,193.114.45.13,success,1622689958",
			"002,27.49.24.26,failure,1622689957"
		)

		val input = stream.map {
			row => {
				val splits = row.split(",", -1).map(_.trim)
				LoginEvent(splits(0), splits(1), splits(2), splits(3).toLong)
			}
		}.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(0)) {
			override def extractTimestamp(t: LoginEvent): Long = t.time * 1000
		}).keyBy(_.userId)

		// 严格，连续两次登录失败
		val pattern = Pattern.begin[LoginEvent]("begin").where(_.loginState.equalsIgnoreCase("failure"))
		  .next("next").where(_.loginState.equalsIgnoreCase("failure"))
		  .within(Time.seconds(2))

		val patternStream = CEP.pattern(input, pattern)

		patternStream.select(new PatternSelectFunction[LoginEvent, LoginWarn] {
			override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarn = {
				val begin = map.get("begin").get(0)
				val next = map.get("next").get(0)
				LoginWarn(begin.userId, begin.time, next.time, next.loginState)
			}
		}).print()

		env.execute(this.getClass.getSimpleName)
	}

	case class LoginEvent(userId: String, ip: String, loginState: String, time: Long)

	case class LoginWarn(userId: String, first: Long, last: Long, loginState: String)

}
