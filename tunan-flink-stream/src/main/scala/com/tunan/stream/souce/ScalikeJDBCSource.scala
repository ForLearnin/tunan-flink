package com.tunan.stream.souce

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class ScalikeJDBCSource extends RichSourceFunction[AlertCount]{

	override def run(ctx: SourceFunction.SourceContext[AlertCount]): Unit = {
		val sql = "select * from alert_count"
		DBs.setupAll()

		DB.readOnly{
			implicit session => {
				SQL(sql).map(rs => {
					val word = rs.string("word")
					val cnt = rs.int("cnt")
					ctx.collect(AlertCount(word,cnt))
				}).list().apply()
			}
		}
	}

	override def cancel(): Unit = {
	}
}
