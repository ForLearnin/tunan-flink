package com.tunan.stream.join

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable.ListBuffer

// 名称,性别,时间
class LeftSource extends RichParallelSourceFunction[(String, String, Long)] {

  var running = true

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[(String, String, Long)]): Unit = {

    var buffer = ListBuffer[(String, String, Long)]()
    buffer.+=(("pk", "M", 50000L)) // [5-6)
    buffer.+=(("pk", "M", 54000L)) // [5-6)
    buffer.+=(("pk", "M", 79900L)) // [7-8)
    buffer.+=(("pk", "M", 115000L)) //[11-12)
    buffer.+=(("xingxing", "M", 100000L)) //[10-11)
    buffer.+=(("xingxing", "M", 109000L))//[10-11)
//    buffer.+=(("pk", "M", 50001L))
    var count = 0
    while (running && count < buffer.length) {
      ctx.collect((buffer(count)._1, buffer(count)._2, buffer(count)._3))
      count += 1
      Thread.sleep(500)
    }
  }
}