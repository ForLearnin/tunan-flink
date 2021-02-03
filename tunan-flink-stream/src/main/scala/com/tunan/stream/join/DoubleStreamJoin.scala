package com.tunan.stream.join

import java.lang
import java.sql.{Connection, PreparedStatement, ResultSet}

import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import com.tunan.utils.{FlinkKafkaSource, MySQLPoolUtils}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.functions.{CoGroupFunction, RichMapFunction}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object DoubleStreamJoin {

    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = FlinkKafkaSource.getEnv
        //        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val studentParameters: ParameterTool = ParameterTool.fromPropertiesFile("tunan-flink-stream/kafka-conf/student.properties")
        val schoolParameters: ParameterTool = ParameterTool.fromPropertiesFile("tunan-flink-stream/kafka-conf/school.properties")

        val studentStream: DataStream[String] = FlinkKafkaSource.createKafkaSource(studentParameters)
        val schoolStream: DataStream[String] = FlinkKafkaSource.createKafkaSource(schoolParameters)

        println("studentStream: " + studentStream.parallelism)

        val delay = 0 // 5s
        val size = 10 // [0,10)

        // wm 触发时间 = size+delay

        val outputTag = new OutputTag[Student]("left-late")

        val rightStream = studentStream
          .filter(row => {
              var flag = true
              if ("" == row || null == row) {
                  flag = false
              }
              flag
          })
          .map(row => {
              Thread.sleep(1000)
              val gson = new Gson()
              val data = JSON.parseObject(row).getJSONObject("data").toString()
              val student = gson.fromJson(data, classOf[Student])
              student
          })
          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Student](Time.seconds(delay)) {
              override def extractTimestamp(element: Student): Long = element.time
          })
          .windowAll(TumblingEventTimeWindows.of(Time.seconds(size)))
          .sideOutputLateData(outputTag)
          .apply(new AllWindowFunction[Student, Student, TimeWindow] {
              override def apply(window: TimeWindow, input: Iterable[Student], out: Collector[Student]): Unit = {
                  for (ele <- input) {
                      out.collect(ele)
                  }
              }
          })

        val leftStream = schoolStream
          .filter(row => {
              var flag = true
              if ("" == row || null == row) {
                  flag = false
              }
              flag
          })
          .map(row => {
              Thread.sleep(1000)
              val gson = new Gson()
              val data = JSON.parseObject(row).getJSONObject("data").toString()
              val student = gson.fromJson(data, classOf[School])
              student
          })
          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[School](Time.seconds(delay)) {
              override def extractTimestamp(element: School): Long = element.time
          })

//        rightStream.print("===")

        val lateDate = rightStream
          .getSideOutput(outputTag)
          .map(new RichMapFunction[Student, Student] {
              val SQL = "select name from study.school where id = ?"

              var conn: Connection = _
              var state: PreparedStatement = _
              var rs: ResultSet = _

              override def open(parameters: Configuration): Unit = {
                  conn = MySQLPoolUtils.getConnection
                  state = conn.prepareStatement(SQL)
              }

              override def close(): Unit = {
                  MySQLPoolUtils.close(rs, state, conn)
              }

              override def map(value: Student): Student = {
                  state.setInt(1, value.school.toInt)
//                  println(" -------- "+value)
                  rs = state.executeQuery()
                  while (rs.next()) {
                      val school = rs.getString("name")
                      value.school = school
                  }
                  value
              }
          })


        // TODO 正常的数据
        rightStream.coGroup(leftStream).where(_.school).equalTo(_.id.toString)
          .window(TumblingEventTimeWindows.of(Time.seconds(size)))
          .apply(new CoGroupFunction[Student, School, Student] {
              override def coGroup(first: lang.Iterable[Student], second: lang.Iterable[School], out: Collector[Student]): Unit = {
                  import scala.collection.JavaConversions._
                  // 左关联，左边必有 (延迟的另外处理)
                  var conn: Connection = null
                  var state: PreparedStatement = null
                  var rs: ResultSet = null
                  try {
                      conn = MySQLPoolUtils.getConnection
                      state = conn.prepareStatement("SELECT name FROM school WHERE id = ?")
                  } catch {
                      case ex: Exception => println("数据库连接异常")
                  }
                  for (e1 <- first) {
                      var joined = false
//                      println(" ========= " + e1)

                      for (e2 <- second) {
                          out.collect(new Student(e1.id, e1.name, e1.age, e1.sex, e2.name, e1.time))
                          joined = true
                      }

                      if (!joined) {
                          state.setInt(1, e1.school.toInt)
                          rs = state.executeQuery()
                          while (rs.next()) {
                              val school = rs.getString("name")
                              out.collect(Student(e1.id, e1.name, e1.age, e1.sex, school, e1.time))
                          }

                      }
                  }
                  MySQLPoolUtils.close(rs, state, conn)
              }
          }).union(lateDate).print()

        env.execute(this.getClass.getSimpleName)
    }
}

class RuozedataAssignerWithPeriodicWatermarks[T <: base](maxAllowedUnorderedTime: Long) extends AssignerWithPeriodicWatermarks[T] {

    val format = FastDateFormat.getInstance("HH:mm:ss")
    var maxTimestamp: Long = 0

    override def extractTimestamp(element: T, previousElementTimestamp: Long): Long = {
        val nowTime = element.time
        maxTimestamp = maxTimestamp.max(nowTime)

        println(element + "," + format.format(nowTime) + "," + format.format(maxTimestamp) + "," + format.format(getCurrentWatermark.getTimestamp))
        nowTime
    }

    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTimestamp - maxAllowedUnorderedTime)
    }
}