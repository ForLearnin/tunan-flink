package com.tunan.stream.souce

import java.sql.{Connection, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class MySQLSource extends RichParallelSourceFunction[AlertCount]{
	var running:Boolean = _
	var connect:Connection = _
	var state:PreparedStatement = _

	override def open(parameters: Configuration): Unit = {
		val sql = "select * from alert_count"
		connect = MySQLUtils.getConnection
		state = connect.prepareStatement(sql)
	}


	override def close(): Unit = {
		MySQLUtils.close(connect,state)
	}

	override def run(ctx: SourceFunction.SourceContext[AlertCount]): Unit = {
		running = true
		while(running){
			val rs = state.executeQuery()
			while(rs.next()){
				val word = rs.getString("word")
				val cnt = rs.getInt("cnt")
				ctx.collect(AlertCount(word,cnt))
			}
		}
	}

	override def cancel(): Unit = {
		running = false
	}

}
