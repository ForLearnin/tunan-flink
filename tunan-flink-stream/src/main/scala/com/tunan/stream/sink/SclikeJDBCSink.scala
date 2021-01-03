package com.tunan.stream.sink

import com.tunan.stream.bean.Access
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

object SclikeJDBCSink {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val files = env.readTextFile("tunan-flink-stream/data/access.txt").setParallelism(1)

        val result = files.map(row => {
            val words = row.split(",").map(_.trim)
            Access(words(0).toLong, words(1), words(2).toLong)
        }).keyBy(x => (x.time, x.domain)).sum(2)

        result.addSink(new CustomMySQL).setParallelism(1)

        env.execute(this.getClass.getSimpleName)
    }
}


class CustomMySQL extends RichSinkFunction[Access] {
    val sql = "REPLACE INTO access(time,domain,traffic) values(?,?,?)"

    override def open(parameters: Configuration): Unit = {
        println(s" ================ ${Thread.currentThread().getId}")
        DBs.setup()
    }


    // 每条数据做一次插入操作，性能低下，需要根据window优化
    override def invoke(access: Access, context: SinkFunction.Context[_]): Unit = {
//        DBs.setup()
        println(s"执行线程: ${Thread.currentThread().getId}")
        DB.localTx ( implicit session =>{
            SQL(sql).bind(access.time,access.domain,access.traffics)
              .update().apply()
        })
    }

    override def close(): Unit = {
        DBs.close()
        println(s"关闭线程: ${Thread.currentThread().getId}")
    }
}
