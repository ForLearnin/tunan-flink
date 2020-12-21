package com.tunan.stream.souce

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.NumberSequenceIterator

/**
 * 单并行度: socketTextStream/fromCollection/fromElements
 * 多并行度: fromParallelCollection/readTextFile/
 * 自定义Source: SourceFunction/RichParallelSourceFunction/ParallelSourceFunction
 */
object SourceApp {

	def main(args: Array[String]) {

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		//		单并行度
		//		fromElements(env)
		//		fromCollection(env)
		//		socketTextStream(env)
		//		多并行度
		//		fromParallelCollection(env)
		//		readTextFile(env)
		val stream = env.addSource(new ScalikeJDBCSource)
		stream.print()
		println("stream的并行度: "+stream.parallelism)
//		val mapDS = stream.map(x => (x.domain, x.traffics))
//		println("Transformation的并行度: "+mapDS.parallelism)
//		mapDS.print()
		env.execute()
	}

	private def readTextFile(env: StreamExecutionEnvironment): Unit = {
		val stream = env.readTextFile("data\\access.txt")
		val map = stream.map(_ + "====> 1001")
		map.print()
	}

	private def fromParallelCollection(env: StreamExecutionEnvironment): Unit = {
		val stream = env.fromParallelCollection(new NumberSequenceIterator(1, 10))
		println(stream.filter(_ > 5).parallelism)
	}


	private def socketTextStream(env: StreamExecutionEnvironment): Unit = {
		val stream = env.socketTextStream("hadoop", 9999)
		stream.filter(x => !"b".equals(x))
	}


	private def fromElements(env: StreamExecutionEnvironment): Unit = {
		val stream = env.fromElements(1, "2", 3L, 4D, 5F)
		println(stream.parallelism)
		println(stream.map(x => x).parallelism)
	}

	private def fromCollection(env: StreamExecutionEnvironment): Unit = {
		val stream = env.fromCollection(List(
			Access(20200514, "www.aa.com", 2000),
			Access(20200514, "www.bb.com", 6000),
			Access(20200514, "www.cc.com", 5000),
			Access(20200514, "www.aa.com", 4000),
			Access(20200514, "www.bb.com", 1000)
		))
		val filterStream = stream.filter(_.traffics > 4000)

		println(filterStream.parallelism)
	}
}
