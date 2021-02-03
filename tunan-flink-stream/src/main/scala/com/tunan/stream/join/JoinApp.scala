package com.tunan.stream.join

import java.lang

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.functions.{CoGroupFunction, JoinFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * 作业：
 * 1）MySQL：把我们测试的数据插入进去
 * 2）通过Maxwell解析binlog日志，传入到Kafka的Topic
 * 3）使用Flink去对接Kafka中的数据
 * json ==> map(json==>(String,String,Long))  转成一个case class
 * 插入和修改
 * 4）使用Flink进行左外连接处理：左表延迟和右表延迟一起实现了                     
 */
object JoinApp {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // event time - delay time = watermark time
        // watermark time > event time 所在区间的上限 即触发窗口计算 (设置延迟时间可以延迟数据中event time触发 watermark的时间，
        // 进而等待更多的event time进入，只要event time在watermark时间内)
        val delay = 0L
        val size = 10

        val left = env.addSource(new LeftSource).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(delay))
        val right = env.addSource(new RightSource).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks(delay))

        // 分开左边正常数据和延迟数据
        val outputTag = new OutputTag[(String, String, Long)]("left-late")
        val leftStream = left.windowAll(TumblingEventTimeWindows.of(Time.seconds(size)))
          .sideOutputLateData(outputTag)
          .apply(new AllWindowFunction[(String, String, Long), (String, String, Long), TimeWindow] {
              override def apply(window: TimeWindow, input: Iterable[(String, String, Long)], out: Collector[(String, String, Long)]): Unit = {
                  for (ele <- input) {
                      out.collect(ele)
                  }
              }
          })
        //                leftStream.print("===")
        //                leftStream.getSideOutput(outputTag).print("---")


        // TODO 左边延迟
        // 处理右边延迟数据  => 查数据库 补全右边的数据
                val leftLateRight = leftStream.getSideOutput(outputTag).map(new RichMapFunction[(String, String, Long), (String, String, String, Long, Long)] {
                    override def open(parameters: Configuration): Unit = super.open(parameters)

                    override def close(): Unit = super.close()

                    override def map(value: (String, String, Long)): (String, String, String, Long, Long) = {
                        (value._1, value._2, "左边延迟查数据库", value._3, 50000L)
                    }
                })
        //
        //        leftLateRight.print("---")


        //    // 正常数据去join  ==>  stream join 的数据必须在一个窗口内才能join的上
                leftStream.coGroup(right).where(_._1).equalTo(_._1)
                  .window(TumblingEventTimeWindows.of(Time.seconds(size)))
                  .apply(new CoGroupFunction[(String, String, Long), (String, String, Long), (String, String, String, Long, Long)] {
                      override def coGroup(first: lang.Iterable[(String, String, Long)], second: lang.Iterable[(String, String, Long)], out: Collector[(String, String, String, Long, Long)]): Unit = {
                          import scala.collection.JavaConversions._
                          // 循环左边的每一条数据
                          for (ele <- first) {
                              var joined = false

                              // 左边的每一条数据 * 右边关联上的数据， ==> 只有关联上了 右边才会有数据，否则右边没有数据，也就不会进行循环操作
                              for (e <- second) {
                                  out.collect((ele._1, ele._2, e._2, ele._3, e._3))
                                  joined = true
                              }

                              // TODO 右边延迟
                              // 如果右边没有数据(可能延迟)，也就是没有进入循环，则去数据库查
                              if (!joined) {
                                  out.collect((ele._1, ele._2, "右边延迟查数据库", ele._3, -999L))
                              }
                          }
                      }
                  }).union(leftLateRight).print() // 合并

        //    left.coGroup(right).where(_._1).equalTo(_._1)
        //      .window(TumblingEventTimeWindows.of(Time.seconds(size)))
        //      .apply(new CoGroupFunction[(String, String, Long), (String, String, Long), (String, String, String, Long, Long)] {
        //        override def coGroup(first: lang.Iterable[(String, String, Long)], second: lang.Iterable[(String, String, Long)], out: Collector[(String, String, String, Long, Long)]): Unit = {
        //          for (ele <- first) {
        //            var joined = false
        //
        //            for (e <- second) {
        //              out.collect((ele._1, ele._2, e._2, ele._3, e._3))
        //              joined = true
        //            }
        //
        //            if (!joined) {
        //              out.collect((ele._1, ele._2, "-", ele._3, -999L))
        //            }
        //          }
        //        }
        //      }).print()


        // inner join  ==>  stream join 的数据必须在一个窗口内才能join的上
//        left.join(right).where(_._1).equalTo(_._1)
//          .window(TumblingEventTimeWindows.of(Time.seconds(size)))
//          .apply(new JoinFunction[(String, String, Long), (String, String, Long), (String, String, String, Long, Long)] {
//              override def join(first: (String, String, Long), second: (String, String, Long)): (String, String, String, Long, Long) = {
//                  (first._1, first._2, second._2, first._3, second._3)
//              }
//          }).print("====")


        env.execute(this.getClass.getSimpleName)
    }

}

private class MyAssignerWithPeriodicWatermarks(maxAllowedUnorderedTime: Long) extends AssignerWithPeriodicWatermarks[(String, String, Long)] {

    val format = FastDateFormat.getInstance("HH:mm:ss")
    var maxTimestamp: Long = 0

    override def extractTimestamp(element: (String, String, Long), previousElementTimestamp: Long): Long = {
        val nowTime = element._3
        maxTimestamp = maxTimestamp.max(nowTime)

        println(element + "," + format.format(nowTime) + "," + format.format(maxTimestamp) + "," + format.format(getCurrentWatermark.getTimestamp))
        nowTime
    }

    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTimestamp - maxAllowedUnorderedTime)
    }
}