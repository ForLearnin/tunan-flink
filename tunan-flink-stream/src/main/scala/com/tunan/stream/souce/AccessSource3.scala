package com.tunan.stream.souce

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.util.Random

class AccessSource3 extends ParallelSourceFunction[Access] {
	val random = new Random()
	val domain: Array[String] = Array("www.aa.com", "www.bb.com", "www.cc.com")


	// 标记
	var running = true

	override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
		while (running) {
			val time = System.currentTimeMillis()
			ctx.collect(Access(time, domain(random.nextInt(domain.length)), random.nextInt(1000)))
		}
	}


	override def cancel(): Unit = {
		running = false
	}
}
