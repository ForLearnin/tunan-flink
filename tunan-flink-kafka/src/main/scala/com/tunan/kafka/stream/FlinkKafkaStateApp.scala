package com.tunan.kafka.stream

import com.tunan.kafka.source.FlinkKafkaSource
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkKafkaStateApp {

    def main(args: Array[String]): Unit = {

        System.setProperty("HADOOP_USER_NAME", "hadoop")
        val parameters: ParameterTool = ParameterTool.fromPropertiesFile("tunan-flink-kafka/conf/parameters.properties")

        val stream: DataStream[String] = FlinkKafkaSource.createKafkaSource(parameters)

        stream
          .map(x => {
              val words = x.split(",").map(_.trim)
              (words(0),words(1).toInt)
          })
          .keyBy(0)
          .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
          .reduce(new ReduceFunction[(String, Int)] {
              override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
                  (value1._1, value1._2 + value2._2)
              }
          }, new ProcessWindowFunction[(String, Int), (String,Int), Tuple, TimeWindow] {
              private var sumState: ValueState[Int] = _

              override def open(parameters: Configuration): Unit = {
                  sumState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("sum_state", classOf[Int]))
              }

              override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[(String,Int)]): Unit = {
                  val tmpState = sumState.value()

                  var currentState = if (null != tmpState) {
                      tmpState
                  } else {
                      0
                  }
                  var key:String = null

                  for(ele <- elements){
                      currentState += ele._2
                      key = ele._1
                  }

                  sumState.update(currentState)
                  out.collect(key,currentState)
              }
          }).print()

        FlinkKafkaSource.getEnv.execute(this.getClass.getSimpleName)
    }
}