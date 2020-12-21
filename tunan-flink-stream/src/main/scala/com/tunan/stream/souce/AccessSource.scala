package com.tunan.stream.souce

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class AccessSource extends SourceFunction[Access]{
	var running = true
	override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
		val random = new Random()
		val domain = Array("www.aa.com", "www.bb.com", "www.cc.com")
		while(running){
			val time = System.currentTimeMillis()
			ctx.collect(Access(time,domain(random.nextInt(domain.length)),random.nextInt(1000)))
		}
	}

	override def cancel(): Unit = {
		running = false
	}
}
