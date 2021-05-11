package com.tunan.stream.bean

import java.sql.{Connection, PreparedStatement, ResultSet}
import com.tunan.utils.MySQLUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

class MySQLSource extends RichSourceFunction[Student] {
	val SQL = "select * from student"
	var conn: Connection = _
	var state: PreparedStatement = _
	var rs: ResultSet = _

	private var isRunning = true

	override def open(parameters: Configuration): Unit = {

		conn = MySQLUtils.getConnection
		state = conn.prepareStatement(SQL)

	}

	override def close(): Unit = {
		conn = null
		state = null
		rs = null
	}

	override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
		rs = state.executeQuery()
		while (rs.next() && isRunning) {
			val id = rs.getInt("id")
			val name = rs.getString("name")
			val age = rs.getInt("age")
			val sex = rs.getString("sex")
			val school = rs.getString("school")
			ctx.collect(Student(id, name, age, sex, school))
		}
	}

	override def cancel(): Unit = {
		isRunning = false
	}
}
