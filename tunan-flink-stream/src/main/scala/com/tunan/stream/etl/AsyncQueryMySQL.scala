package com.tunan.stream.etl

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.{Callable, ExecutorService, TimeUnit}

import com.alibaba.druid.pool.DruidDataSource
import com.tunan.stream.bean.Student
import com.tunan.utils.MySQLPoolUtils
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}

import scala.concurrent.{ExecutionContext, Future}

object AsyncQueryMySQL {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val stream = env.socketTextStream("aliyun", 9999)

        val result = AsyncDataStream.unorderedWait(stream, new AsyncMySQLRequest(), 1000, TimeUnit.MILLISECONDS, 10).setParallelism(2)
        result.print().setParallelism(2)
        env.execute(this.getClass.getSimpleName)
    }
}


class AsyncMySQLRequest extends RichAsyncFunction[String, Student] {
    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

    var executorService: ExecutorService = _
    var dataSource: DruidDataSource = _


    override def open(parameters: Configuration): Unit = {
        println(" ============= ===============")

        executorService = java.util.concurrent.Executors.newFixedThreadPool(20)

        //  拿的是一个dataSource
        //  dataSource = MySQLPoolUtils.getDataSource

        //  dataSource的个数由并行度决定
        dataSource = new DruidDataSource
        dataSource.setDriverClassName(MySQLPoolUtils.DRIVER)
        dataSource.setUsername(MySQLPoolUtils.USER)
        dataSource.setPassword(MySQLPoolUtils.PASSWORD)
        dataSource.setUrl(MySQLPoolUtils.RUL)
        dataSource.setInitialSize(5)
        dataSource.setMinIdle(10)
        dataSource.setMaxActive(20)

        println(dataSource.getName)

    }

    override def close(): Unit = {
        dataSource.close()
        executorService.shutdown()
    }


    override def asyncInvoke(input: String, resultFuture: ResultFuture[Student]): Unit = {
        val future: java.util.concurrent.Future[Student] = executorService.submit(new Callable[Student] {
            override def call(): Student = {
                query(input.toInt)
            }
        })

        val eventualString = Future {
            future.get()
        }

        eventualString.onSuccess {
            case result: Student => resultFuture.complete(Iterable(result))
        }

    }

    def query(id: Int): Student = {
        var name: String = ""
        var age: Int = 0
        var sex: String = ""
        var school: String = ""

        val sql = "select id,name,age,sex,school from study.student where id = ?"

        var conn: Connection = null
        var pstmt: PreparedStatement = null
        var rs: ResultSet = null

        try {
            conn = dataSource.getConnection
            pstmt = conn.prepareStatement(sql)
            pstmt.setInt(1, id)
            rs = pstmt.executeQuery()
            while (rs.next()) {
                name = rs.getString("name")
                age = rs.getInt("age")
                sex = rs.getString("sex")
                school = rs.getString("school")
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            MySQLPoolUtils.close(rs, pstmt, conn)
        }
        Student(id, name, age, sex, school)
    }
}