package com.tunan.stream.window

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowsApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 	EventTime
        //	env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val stream = env.socketTextStream("aliyun", 9999)

        // TODO ByKey
        // 滚动 按次数提交window
        //		countWindowByKey(stream)
        // 滚动 按时间提交window
        //		timeWindowByKey(stream)
        // 滑动window
        //		slideWindowByKey(stream)

        // TODO Non ByKey
        // 滚动 按次数提交window
        //		countWindowAll(stream)
        // 滚动 按时间提交window
        //		timeWindowAll(stream)
        // 滑动window
        //		slideWindowAll(stream)


        // TODO Window Function
        // reduce
        //		reduceFunction(stream)
        // agg
        //		aggregateFunction(stream)
        // process
        		processFunction(stream)


        env.execute(this.getClass.getSimpleName)
    }

    private def processFunction(stream: DataStream[String]) = {
        stream
          .map(row => {
              val words = row.split(",")
              (words(0), words(1).toInt)
          })
          .keyBy(_._1)
          .timeWindow(Time.seconds(5))
          .process(new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {

              override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {

                  var maxValue = Int.MinValue
                  for (ele <- elements) {
                      maxValue = ele._2.max(maxValue)
                  }

                  out.collect(s"最大值是: ${maxValue}, 开始时间: ${new Timestamp(context.window.getStart)},结束时间: ${new Timestamp((context.window.getEnd))}")
              }
          })
          .print()
    }

    private def aggregateFunction(stream: DataStream[String]) = {
        stream
          .map(row => {
              val words = row.split(",")
              (words(0), words(1).toInt)
          })
          .keyBy(_._1)
          .timeWindow(Time.seconds(5))
          .aggregate(new AggregateFunction[(String, Int), (Int, Int), Double] {
              override def createAccumulator(): (Int, Int) = (0, 0)

              override def add(value: (String, Int), accumulator: (Int, Int)): (Int, Int) = {
                  (accumulator._1 + 1, accumulator._2 + value._2)
              }

              override def getResult(accumulator: (Int, Int)): Double = {
                  accumulator._2 / accumulator._1.toDouble
              }

              override def merge(a: (Int, Int), b: (Int, Int)): (Int, Int) = ???
          })
          .print()
    }

    private def reduceFunction(stream: DataStream[String]) = {
        stream
          .map(x => (x, 1))
          .keyBy(_._1)
          .timeWindow(Time.seconds(5))
          .reduce((a, b) => {
              println(s"增量reduce ${a},${b}")
              (a._1, b._2 + a._2)
          })
          .print()
    }

    private def slideWindowAll(stream: DataStream[String]) = {
        stream
          .map(_.toInt)
          .timeWindowAll(Time.seconds(6), Time.seconds(3))
          .sum(0)
          .print()
    }

    private def countWindowAll(stream: DataStream[String]) = {
        stream
          .map(_.toInt)
          .countWindowAll(3)
          .sum(0)
          .print()
    }

    private def timeWindowAll(stream: DataStream[String]) = {
        stream
          .map(_.toInt)
          .timeWindowAll(Time.seconds(3))
          .sum(0)
          .print()
    }

    private def slideWindowByKey(stream: DataStream[String]) = {
        stream
          .map(x => (x, 1))
          .keyBy(0)
          .timeWindow(Time.seconds(6), Time.seconds(3))
          .sum(1)
          .print()
    }

    private def countWindowByKey(stream: DataStream[String]) = {
        stream
          .map(x => (x, 1))
          .keyBy(0)
          .countWindow(5)
          .sum(1)
          .print()
    }

    private def timeWindowByKey(stream: DataStream[String]) = {
        stream
          .map(x => (x, 1))
          .keyBy(0)
          .timeWindow(Time.seconds(3))
          .sum(1)
          .print()
    }
}
