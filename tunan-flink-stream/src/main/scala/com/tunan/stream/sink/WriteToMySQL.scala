package com.tunan.stream.sink

import java.sql.{Connection, PreparedStatement}

import com.tunan.stream.bean.Access
import com.tunan.utils.MySQLUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

object WriteToMySQL {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val files = env.readTextFile("tunan-flink-stream/data/access.txt")

		val result = files.map(row => {
			val words = row.split(",").map(_.trim)
			Access(words(0).toLong, words(1), words(2).toLong)
		}).keyBy(x => (x.time, x.domain)).sum(2)

		result.addSink(new CustomMySQL)

		class CustomMySQL extends RichSinkFunction[Access]{
			var conn:Connection = _
			var pstate:PreparedStatement =_

			override def open(parameters: Configuration): Unit = {
				conn = MySQLUtils.getConnection
				pstate = conn.prepareStatement("REPLACE INTO access(time,domain,traffic) values(?,?,?)")
			}


			// 每条数据做一次插入操作，性能低下，需要根据window优化
			override def invoke(value: Access, context: SinkFunction.Context[_]): Unit = {
				pstate.setLong(1,value.time)
				pstate.setString(2,value.domain)
				pstate.setLong(3,value.traffics)

				pstate.execute()

				// 如果不为-1 则代表有更新
				println(pstate.getUpdateCount)
			}

			override def close(): Unit = {
				MySQLUtils.close(conn,pstate,null)
			}
		}



		env.execute(this.getClass.getSimpleName)
	}
}
