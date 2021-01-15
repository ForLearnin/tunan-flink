package com.tunan.item.service

import java.sql.{Connection, PreparedStatement}

import com.tunan.item.domain.Access
import com.tunan.utils.{PhoenixUtils, TimeUtils}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object VisitorsRadar {

    var CURRENT_TIME_MILLIS: Long = TimeUtils.getTimeInMillis
    var DIFF_TIME: Long = 86400000L

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        val stream = env.readTextFile("file:///sparkspace/tunan-flink/tunan-flink-item/data/file.txt")
        val stream = env.socketTextStream("aliyun",9999)

        stream.map(row => {
            val words = row.split(",")
            Access(words(0), words(1), words(2), words(3), words(4),
                words(5), words(6), words(7), 0, words(8).toInt, 0, words(9),
                words(10), words(11), words(12), words(13))
        })
          .keyBy(x => (x.belonger_id, x.member_id, x.source_plate))
          .flatMap(new RichFlatMapFunction[Access, Access] {
              var sumTime: ValueState[Long] = _
              var logCount: ValueState[Long] = _


              override def open(parameters: Configuration): Unit = {

                  sumTime = getRuntimeContext.getState(new ValueStateDescriptor[Long]("sumTime", classOf[Long]))
                  logCount = getRuntimeContext.getState(new ValueStateDescriptor[Long]("logCount", classOf[Long]))
              }

              override def flatMap(value: Access, out: Collector[Access]): Unit = {

                  // 只保存当天的累计访问次数
                  if (System.currentTimeMillis() - CURRENT_TIME_MILLIS > DIFF_TIME) {
                      CURRENT_TIME_MILLIS = TimeUtils.getTimeInMillis
                      logCount.clear()
                  }

                  val tmpSumTime = sumTime.value()
                  val tmpLogCount = logCount.value()

                  val currentSumTime = if (null == tmpSumTime) {
                      0L
                  } else {
                      tmpSumTime
                  }
                  val currentLogCount = if (null == tmpLogCount) {
                      0L
                  } else {
                      tmpLogCount
                  }

                  val newSumTime = currentSumTime + value.stay_time
                  val newLogCount = currentLogCount + 1

                  sumTime.update(newSumTime)
                  logCount.update(newLogCount)

                  value.visit_num = newLogCount
                  value.total_stay_time = newSumTime

                  out.collect(value)
              }
          }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
          .process(new ProcessAllWindowFunction[Access, String, TimeWindow] {
              var conn: Connection = _
              var state: PreparedStatement = _
              val insertListSQL = "UPSERT INTO UF.VISIT_LIST_DAY(BELONGER_ID, VISIT_TIME, MEMBER_ID, SOURCE_PLATE, THUMB, NAME, SOURCE_PLATFORM, CONTENT, VISIT_NUM, STAY_TIME, TOTAL_STAY_TIME, ACT, TITLE, HOME_IMG, HOUSE_NUMBER, STRESS_NAME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

              override def open(parameters: Configuration): Unit = {
                  conn = PhoenixUtils.getConnection
                  state = conn.prepareStatement(insertListSQL)
                  conn.setAutoCommit(false)
              }

              override def close(): Unit = {
                  conn.close()
              }

              override def process(context: Context, elements: Iterable[Access], out: Collector[String]): Unit = {
                  for (ele <- elements) {
                      state.setString(1, ele.belonger_id)
                      state.setString(2, ele.visit_time)
                      state.setString(3, ele.member_id)
                      state.setString(4, ele.source_plate)
                      state.setString(5, ele.thumb)
                      state.setString(6, ele.name)
                      state.setString(7, ele.source_platform)
                      state.setString(8, ele.content)
                      state.setLong(9, ele.visit_num)
                      state.setLong(10, ele.stay_time)
                      state.setLong(11, ele.total_stay_time)
                      state.setString(12, ele.act)
                      state.setString(13, ele.title)
                      state.setString(14, ele.home_img)
                      state.setString(15, ele.house_number)
                      state.setString(16, ele.stress_name)
                      state.addBatch()
                  }
                  println("=============")
                  state.executeBatch()
                  conn.commit()

                  out.collect("写入Phoenix成功")
              }

          }).print()

        env.execute(this.getClass.getSimpleName)
    }
}


class SimpleToPhoenix extends RichSinkFunction[Access] {
    var conn: Connection = _
    var state: PreparedStatement = _
    val insertListSQL = "UPSERT INTO UF.VISIT_LIST_DAY(BELONGER_ID, VISIT_TIME, MEMBER_ID, SOURCE_PLATE, THUMB, NAME, SOURCE_PLATFORM, CONTENT, VISIT_NUM, STAY_TIME, TOTAL_STAY_TIME, ACT, TITLE, HOME_IMG, HOUSE_NUMBER, STRESS_NAME) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"


    override def open(parameters: Configuration): Unit = {
        conn = PhoenixUtils.getConnection
        state = conn.prepareStatement(insertListSQL)
    }

    override def close(): Unit = {
        conn.close()
    }

    override def invoke(ele: Access, context: SinkFunction.Context): Unit = {
        state.setString(1, ele.belonger_id)
        state.setString(2, ele.visit_time)
        state.setString(3, ele.member_id)
        state.setString(4, ele.source_plate)
        state.setString(5, ele.thumb)
        state.setString(6, ele.name)
        state.setString(7, ele.source_platform)
        state.setString(8, ele.content)
        state.setLong(9, ele.visit_num)
        state.setLong(10, ele.stay_time)
        state.setLong(11, ele.total_stay_time)
        state.setString(12, ele.act)
        state.setString(13, ele.title)
        state.setString(14, ele.home_img)
        state.setString(15, ele.house_number)
        state.setString(16, ele.stress_name)
        state.executeUpdate()
        conn.commit()
    }
}