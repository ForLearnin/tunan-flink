package com.tunan.stream.join

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable.ListBuffer

class RightSource extends RichParallelSourceFunction[(String, String, Long)] {

  var running = true

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String, Long)]): Unit = {

    var buffer = ListBuffer[(String, String, Long)]()
    buffer.+=(("pk", "BJ", 50000L))  // [5-6)
    buffer.+=(("xingxing", "SH", 109000L)) //[10-11)

    var count = 0
    while (running && count < buffer.length) {
      ctx.collect((buffer(count)._1, buffer(count)._2, buffer(count)._3))
      count += 1
      Thread.sleep(1000)
    }
  }
}