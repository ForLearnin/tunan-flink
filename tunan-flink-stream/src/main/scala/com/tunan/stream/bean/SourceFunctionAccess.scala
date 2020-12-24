package com.tunan.stream.bean

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class SourceFunctionAccess extends SourceFunction[Access]{

	var RUNNING = true

	// 单并行度
	override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
		val random = new Random()
		val domain = Array("www.aa.com", "www.bb.com", "www.cc.com")
		while(RUNNING){
			domain(random.nextInt(domain.length))
			val time = System.currentTimeMillis()
			ctx.collect(Access(time,domain(random.nextInt(domain.length)),random.nextInt(5000)))
		}
	}

	override def cancel(): Unit = {
		RUNNING = false
	}
}
