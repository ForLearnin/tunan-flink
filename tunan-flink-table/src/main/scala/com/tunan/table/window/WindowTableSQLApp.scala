package com.tunan.table.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Slide, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object WindowTableSQLApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 时间，用户，商品，价格
        val input = env.fromElements(
            "1000,pk,Spark,75",
            "2000,pk,Flink,65",
            "2000,3ben,蜡烛,3",
            "3000,pk,CDH,65",
            "9999,3ben,皮鞭,65",
            "19999,pk,Hive,45"
        ).map(x => {
            val splits = x.split(",")
            val time = splits(0).toLong
            val user = splits(1)
            val product = splits(2)
            val money = splits(3).toDouble
            (time, user, product, money)
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String, String, Double)](Time.seconds(0)) {
            override def extractTimestamp(element: (Long, String, String, Double)): Long = element._1
        })

        val tableEnv = StreamTableEnvironment.create(env)
        tableEnv.createTemporaryView("access",input,'time,'user,'product,'money,'rowtime.rowtime)

        // tumble api
/*        val resultTable= tableEnv.from("access")
          .window(Tumble.over("10.seconds").on("rowtime").as("win"))
          .groupBy("user,win")
          .select("user,win.start,win.end,win.rowtime,money.sum as total")*/

        // tumble sql
//        val sql =
//            """
//              |select user,sum(money) as total,
//              |tumble_start(rowtime,interval '10' second) as win_start,
//              |tumble_end(rowtime,interval '10' second) as win_end
//              |from access
//              |group by user,tumble(rowtime,interval '10' second)
//              |""".stripMargin

        // api
//                val resultTable= tableEnv.from("access")
//                  .window(Slide.over("10.seconds").every("2.seconds").on("rowtime").as("win"))
//                  .groupBy("user,win")
//                  .select("user,win.start,win.end,win.rowtime,money.sum as total")

        // sql
                val sql =
                    """
                      |select user,sum(money) as total,
                      |hop_start(rowtime,interval '2' second,interval '10' second) as win_start,
                      |hop_end(rowtime,interval '2' second,interval '10' second) as win_end
                      |from access
                      |group by user,hop(rowtime,interval '2' second,interval '10' second)
                      |""".stripMargin

        val resultTable = tableEnv.sqlQuery(sql)

        val value = tableEnv.toRetractStream[Row](resultTable)
        value.addSink(x => {
            val value1: Row = x._2
            val a = value1.getField(0)
            val b = value1.getField(1)
            val c = value1.getField(2)
            val d = value1.getField(3)
            println(a,b,c,d)
        })

        env.execute(this.getClass.getSimpleName)

    }
}
