package com.tunan.stream.sink

import com.tunan.stream.bean.Access
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object WriteToText {

	def main(args: Array[String]): Unit = {

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		val files = env.readTextFile("tunan-flink-stream/data/access.txt")

		val result = files.map(row => {
			val words = row.split(",").map(_.trim)
			Access(words(0).toLong, words(1), words(2).toLong)
		}).keyBy(x => (x.time, x.domain)).sum(2)

		result.addSink(StreamingFileSink.forRowFormat(
			new Path("tunan-flink-stream/out/text"),
			new SimpleStringEncoder[Access]).build())

		env.execute(this.getClass.getSimpleName)
	}
}
