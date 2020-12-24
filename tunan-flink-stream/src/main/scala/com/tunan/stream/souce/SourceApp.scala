package com.tunan.stream.souce

import com.tunan.stream.bean._
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.NumberSequenceIterator

/**
 * 单并行度: socketTextStream/fromCollection/fromElements
 * 多并行度: fromParallelCollection/readTextFile/
 * 自定义Source: SourceFunction/ParallelSourceFunction/RichParallelSourceFunction
 */
object SourceApp {

	def main(args: Array[String]) {

		val env = StreamExecutionEnvironment.getExecutionEnvironment

		//		socketTextStream(env)
		//		readTextFile(env)
		//		fromCollection(env)
		//		fromParallelCollection(env)
		//		sourceFunctionAccess(env)
		//		parallelSourceFunctionAccess(env)
		//		richParallelSourceFunctionAccess(env)
		//		mysqlSourceFunction(env)
		scalikejdbcSourceFunction(env)

		env.execute()
	}

	private def scalikejdbcSourceFunction(env: StreamExecutionEnvironment): Unit = {
		val sourceFunction = env.addSource(new ScalikeJDBCSource)
		println("默认并行度: " + sourceFunction.parallelism) // 1
		val result = sourceFunction.map(x => x)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}

	private def mysqlSourceFunction(env: StreamExecutionEnvironment): Unit = {
		val sourceFunction = env.addSource(new MySQLSource).setParallelism(1)
		println("默认并行度: " + sourceFunction.parallelism) // 1
		val result = sourceFunction.map(x => x)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}


	private def richParallelSourceFunctionAccess(env: StreamExecutionEnvironment): Unit = {
		val sourceFunction = env.addSource(new RichParallelSourceFunctionAccess).setParallelism(2)
		println("默认并行度: " + sourceFunction.parallelism) // 1
		val result = sourceFunction.map(x => x)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}

	private def parallelSourceFunctionAccess(env: StreamExecutionEnvironment): Unit = {
		val sourceFunction = env.addSource(new ParallelSourceFunctionAccess()).setParallelism(1)
		println("默认并行度: " + sourceFunction.parallelism) // 1
		val result = sourceFunction.filter(_.traffics > 3000)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}

	private def sourceFunctionAccess(env: StreamExecutionEnvironment): Unit = {
		val sourceFunction = env.addSource(new SourceFunctionAccess())
		println("默认并行度: " + sourceFunction.parallelism) // 1
		val result = sourceFunction.filter(_.traffics > 3000)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}

	private def fromParallelCollection(env: StreamExecutionEnvironment): Unit = {
		val parallelCollection = env.fromParallelCollection(new NumberSequenceIterator(1, 10))
		println("默认并行度: " + parallelCollection.parallelism) // 4
		val result = parallelCollection.filter(_ > 5)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}

	private def fromCollection(env: StreamExecutionEnvironment): Unit = {
		val collect = env.fromCollection(
			Access(20200514, "www.aa.com", 2000L) ::
			  Access(20200514, "www.bb.com", 6000L) ::
			  Access(20200514, "www.cc.com", 5000L) ::
			  Access(20200514, "www.aa.com", 4000L) ::
			  Access(20200514, "www.bb.com", 1000L) :: Nil
		)
		println("默认并行度: " + collect.parallelism) // 1
		val result = collect.map(x => ((x.time, x.domain), x.traffics)).keyBy(_._1).sum(1)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}

	private def readTextFile(env: StreamExecutionEnvironment) = {
		val file = env.readTextFile("tunan-flink-stream/data/access.txt")
		println("默认并行度: " + file.parallelism) // 4
		val result = file.map(row => {
			val words = row.split(",").map(_.trim)
			((words(0), words(1)), words(2).toInt)
		}).keyBy(x => (x._1._1, x._1._2)).sum(1)

		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}

	private def socketTextStream(env: StreamExecutionEnvironment) = {
		val socket = env.socketTextStream("aliyun", 9999)
		println("默认并行度: " + socket.parallelism) // 1
		val result = socket.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)
		println("转换并行度: " + result.parallelism) // 4
		result.print()
	}
}
