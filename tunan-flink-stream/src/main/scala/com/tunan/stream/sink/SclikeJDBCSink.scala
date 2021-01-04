package com.tunan.stream.sink

import com.tunan.stream.bean.Access
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

import scala.collection.mutable.ListBuffer

object SclikeJDBCSink {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //        val files = env.readTextFile("tunan-flink-stream/data/access.txt")
        val files = env.socketTextStream("aliyun", 9999).filter(_.trim.nonEmpty)


        // keyBy是带状态的
        val result = files.map(row => {
            val words = row.split(",").map(_.trim)
            Access(words(0).toLong, words(1), words(2).toLong)
        }).keyBy(x => (x.time, x.domain)).timeWindow(Time.seconds(3)).process(new ProcessWindowFunction[Access, Access, (Long, String), TimeWindow] {

//            private var sum: ValueState[Long] = _
            private val countStateDesc = new ValueStateDescriptor[Long]("count", classOf[Long])

            override def process(key: (Long, String), context: Context, elements: Iterable[Access], out: Collector[Access]): Unit = {

                val sum = context.windowState.getState(countStateDesc).value()

                var currentState = if (0 != sum) {
                    sum
                } else {
                    0L
                }

                for (ele <- elements) {
                    currentState += ele.traffics
                }

                context.windowState.getState(countStateDesc).update(currentState)

                out.collect(Access(key._1,key._2,currentState))

            }
        }).print()

//        result.addSink(new CustomMySQL).setParallelism(4)

        env.execute(this.getClass.getSimpleName)
    }
}


class CustomMySQL extends RichSinkFunction[Access] {
    val sql = "REPLACE INTO access(time,domain,traffic) values(?,?,?)"

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        println(s"初始化线程: ${Thread.currentThread().getId}")
        DBs.setupAll()
    }


    // 每条数据做一次插入操作，性能低下，需要根据window优化
    override def invoke(access: Access, context: SinkFunction.Context[_]): Unit = {
        println(s"执行线程: ${Thread.currentThread().getId}")
        val buffer = new ListBuffer[Seq[Any]]
        buffer += Seq(access.time, access.domain, access.traffics)

        insertBatchData(sql, buffer)
        println(access)
    }


    private def insertBatchData(sql: String, params: Seq[Seq[Any]]) = {
        DB.localTx(implicit session => {
            SQL(sql).batch(params: _*).apply()
        })
    }

    override def close(): Unit = {
        super.close()
        DBs.closeAll()
        println(s"关闭线程: ${Thread.currentThread().getId}")
    }
}
