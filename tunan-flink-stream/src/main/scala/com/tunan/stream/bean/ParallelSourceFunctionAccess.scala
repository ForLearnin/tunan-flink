package com.tunan.stream.bean

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random

class ParallelSourceFunctionAccess extends ParallelSourceFunction[Access]{

	var RUNNING = true

	override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
		val random = new Random()
		val domain = Array("www.aa.com", "www.bb.com", "www.cc.com")
		println("=====================================================")
		while(RUNNING){
			domain(random.nextInt(domain.length))
			val time = System.currentTimeMillis()
			ctx.collect(Access(time,domain(random.nextInt(domain.length)),random.nextInt(5000)))
		}
	}

	override def cancel(): Unit = RUNNING = false
}
