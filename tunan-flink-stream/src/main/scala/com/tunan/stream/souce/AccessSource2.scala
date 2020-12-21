package com.tunan.stream.souce

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class AccessSource2 extends RichParallelSourceFunction[Access]{
	var random:Random = _
	var domain:Array[String] = _

	// 做初始化操作
	override def open(parameters: Configuration): Unit = {
		random = new Random()
		domain = Array("www.aa.com", "www.bb.com", "www.cc.com")

		println("========open.invoke===========")
	}
	// 标记
	var running = true
	override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
		while(running){
			val time = System.currentTimeMillis()
			ctx.collect(Access(time,domain(random.nextInt(domain.length)),random.nextInt(1000)))
		}
	}

	override def close(): Unit = super.close()

	override def cancel(): Unit = {
		running = false
	}
}
