package com.tunan.flink.cep

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration


/*
	有些用户会在官网下单，随后又会退款，并且在联盟下单，我们需要把这部分用户标记为风险用户
 */
import java.util

object CEPRun {
	def main(args: Array[String]): Unit = {
		val env = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)

		/*
		* 监控到所有先在官网买课的有用户退款后又去联盟购课的用户
		*/
		val elements = env.fromElements(
			"600,gw,pay,1622689938",
			"100,gw,pay,1622689952",
			"100,gw,refund,1622689953",
			"100,lm,pay,1622689954",
			"200,gw,pay,1622689955",
			"300,gw,pay,1622689956",
			"400,gw,pay,1622689957",
			"600,gw,refund,1622689959",
			"600,lm,pay,1622689960"
		)

		val input = elements.map(row => {
			val splits = row.split(",")
			Run(splits(0), splits(1), splits(2),splits(3).toLong)
		}).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ZERO)
			.withTimestampAssigner(new SerializableTimestampAssigner[Run] {
				override def extractTimestamp(t: Run, l: Long): Long = t.time
			})).keyBy(_.user)

		val pattern = Pattern.begin[Run]("first").where(x => x.behavior.equalsIgnoreCase("pay") && x.from.equalsIgnoreCase("gw"))
		  .followedBy("next").where(x => x.behavior.equalsIgnoreCase("refund") && x.from.equalsIgnoreCase("gw"))
		  .followedBy("last").where(x => x.behavior.equalsIgnoreCase("pay") && x.from.equalsIgnoreCase("lm"))
		  .within(Time.seconds(60))

		val stream = CEP.pattern(input, pattern)

		// 通知
		val output = new OutputTag[Notice]("notice")

		val result = stream.select(output, new PatternTimeoutFunction[Run, Notice] {
			override def timeout(map: util.Map[String, util.List[Run]], l: Long): Notice = {
				val first = map.get("first").get(0)
				Notice(first.user, first.from,first.from,"INFO: 安全用户")
			}
		}, new PatternSelectFunction[Run, Notice] {
			override def select(map: util.Map[String, util.List[Run]]): Notice = {
				val first = map.get("first").get(0)
				val last = map.get("last").get(0)
				Notice(first.user, first.from, last.from, "ERROR: 风险用户")
			}
		})

		result.print()
		result.getSideOutput(output).print()


		env.execute(this.getClass.getSimpleName)
	}
	case class Run(user:String,from:String,behavior:String,time:Long)
	case class Notice(user:String,first_from:String,next_from:String,context:String)
}
