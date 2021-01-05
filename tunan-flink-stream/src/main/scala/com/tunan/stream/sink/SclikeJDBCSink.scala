package com.tunan.stream.sink

import java.sql.{Connection, PreparedStatement}

import com.tunan.stream.bean.Access
import com.tunan.utils.MySQLUtils
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

//                val files = env.readTextFile("tunan-flink-stream/data/access.txt")
        val files = env.socketTextStream("aliyun", 9999).filter(_.trim.nonEmpty)


        // keyBy是带状态的
        val result = files.map(row => {
            val words = row.split(",").map(_.trim)
            Access(words(0).toLong, words(1), words(2).toLong)
        }).keyBy(x => (x.time, x.domain)).timeWindow(Time.seconds(3))
          .process(new ProcessWindowFunction[Access, Access, (Long, String), TimeWindow] {

              // 问题1： 为什么结果没有更新到这里
            private var sum: ValueState[Long] = _
            private val countStateDesc = new ValueStateDescriptor[Long]("count", classOf[Long])

            override def process(key: (Long, String), context: Context, elements: Iterable[Access], out: Collector[Access]): Unit = {

                sum = context.windowState.getState(countStateDesc)

                var currentState = if (null != sum) {
                    sum.value()
                } else {
                    0L
                }

                for (ele <- elements) {
                    currentState += ele.traffics
                }

                sum.update(currentState)

                out.collect(Access(key._1,key._2,currentState))
            }
        })
//          .sum(2)
//          .print()
//
        result.addSink(new CustomMySQLByJDBC)

        env.execute(this.getClass.getSimpleName)
    }
}


// 问题2： 为什么Scalike读文件会没有数据进来,
// 读取文件报错: Connection pool is not yet initialized.(name:'default)，
// 单线下不会报错，怀疑是线程不安全
class CustomMySQL extends RichSinkFunction[Access] {
    val sql = "REPLACE INTO access(time,domain,traffic) VALUES(?,?,?)"

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


// 使用JDBC的方式可以读取文件数据写入MySQL,无论是scalike还是jdbc都是拿着线程不还
class CustomMySQLByJDBC extends RichSinkFunction[Access] {
    val sql = "REPLACE INTO access(time,domain,traffic) VALUES(?,?,?)"

    var conn:Connection = _
    var pstate: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        conn = MySQLUtils.getConnection
        pstate = conn.prepareCall(sql)
        println(s"初始化线程: ${Thread.currentThread().getId}")
    }


    // 每条数据做一次插入操作，性能低下，需要根据window优化
    override def invoke(access: Access, context: SinkFunction.Context[_]): Unit = {
        println(s"执行线程: ${Thread.currentThread().getId}")

        pstate.setLong(1,access.time)
        pstate.setString(2,access.domain)
        pstate.setLong(3,access.traffics)

        pstate.execute()
    }


    override def close(): Unit = {
        super.close()
        MySQLUtils.close(conn,pstate,null)
        println(s"关闭线程: ${Thread.currentThread().getId}")
    }
}