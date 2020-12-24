package com.tunan.stream.bean

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class RichParallelSourceFunctionAccess extends RichParallelSourceFunction[Access] {

	var RUNNING = true

	override def open(parameters: Configuration): Unit = {
		println(" ==========open.invoke============ ")
	}

	override def close(): Unit = {
		println(" ==========close.invoke============ ")
	}

	override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
		val random = new Random()
		val domain = Array("www.aa.com", "www.bb.com", "www.cc.com")
		for (i <- 1 to 10){
			domain(random.nextInt(domain.length))
			val time = System.currentTimeMillis()
			ctx.collect(Access(time,domain(random.nextInt(domain.length)),random.nextInt(5000)+i))
		}
	}

	override def cancel(): Unit = RUNNING = false
}
