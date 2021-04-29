package com.tunan.stream.sink

import com.tunan.stream.bean.Access
import com.tunan.utils.MysqlConnectPool
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import scalikejdbc.{DB, NoExtractor, SQL, WrappedResultSet, _}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

object SclikeJDBCSinkTest {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val files = env.readTextFile("tunan-flink-stream/data/access.txt")

        val result = files.map(row => {
            val words = row.split(",").map(_.trim)
            Access(words(0).toLong, words(1), words(2).toLong)
        }).keyBy(x => (x.time, x.domain)).sum(2)

        result.addSink(new com.tunan.stream.sink.MysqlCustomerSink())

        env.execute(this.getClass.getSimpleName)
    }
}

class MysqlCustomerSink extends RichSinkFunction[Access] {

    var deptDAO: DeptDAO = _

    override def open(parameters: Configuration): Unit = {


        deptDAO = new DeptDAO
    }

    override def invoke(value: Access): Unit = {
        val list = new ListBuffer[Seq[Any]]
        list.append(Seq(value.time, value.domain, value.traffics))
        deptDAO.bacthInsert(list)
    }


    override def close(): Unit = {
        super.close()
    }

}


class DeptDAO extends BaseDAO {

    override def bacthInsert(list: ListBuffer[Seq[Any]]): Unit = {
        val sql = sql"REPLACE INTO access(time,domain,traffic) values(?,?,?)"
        db.batchInsert(sql, list.toSeq)
    }

}

trait BaseDAO extends Serializable {

    val db = DBOperations

    def bacthInsert(list: ListBuffer[Seq[Any]])

    def bacthInsertByName(list: ListBuffer[Seq[(Symbol, Any)]]): Unit = {
        val sqlStr = sql""
        db.batchByNameInsert(sqlStr, list.toSeq)
    }
}

object DBOperations extends Serializable {

    def batchInsert(sqlStr: String, params: ListBuffer[Seq[Any]]): Unit = {
        DB localTx { implicit session =>
            sql"$sqlStr".batch(params: _*).apply()
        }
    }

    def batchInsert(sql: SQL[Nothing, NoExtractor], params: Seq[Seq[Any]]): Unit = {
        DB localTx { implicit session =>
            sql.batch(params: _*).apply()
        }
    }

    def batchByNameInsert(sql: SQL[Nothing, NoExtractor], params: Seq[Seq[(Symbol, Any)]]): Unit = {
        DB localTx { implicit session =>
            sql.batchByName(params: _*).apply()
        }
    }

    def queryAll[A](sql: SQL[Nothing, NoExtractor], f: WrappedResultSet => A): immutable.Seq[Any] = {
        DB readOnly { implicit session =>
            sql.map(f).list().apply()
        }
    }

}