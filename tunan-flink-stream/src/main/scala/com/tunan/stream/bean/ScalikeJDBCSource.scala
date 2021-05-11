package com.tunan.stream.bean

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

class ScalikeJDBCSource extends RichSourceFunction[Student]{
	val sql = "select * from student"

	override def open(parameters: Configuration): Unit = {
		DBs.setup()
		println("初始化一个")
	}

	override def close(): Unit ={
		DBs.close()
		println("断开连接一个")
	}

	override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
		DB.readOnly(implicit session => {
			SQL(sql).map(rs => {
				val id = rs.int("id")
				val name = rs.string("name")
				val age = rs.int("age")
				val sex = rs.string("sex")
				val school = rs.string("school")
				ctx.collect(Student(id, name, age, sex, school))
			}).list().apply()
		})

	}

	override def cancel(): Unit = ???
}
