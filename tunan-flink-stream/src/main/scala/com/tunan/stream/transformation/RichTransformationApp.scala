package com.tunan.stream.transformation

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.tunan.utils.MySQLUtils
import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object RichTransformationApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(2)
        val file = env.readTextFile("tunan-flink-stream/data/student.txt")
        file.filter(new MyRichFilterFunction("长郡一中")).print()

        env.execute(this.getClass.getSimpleName)
    }
}

class MyRichMapFunction extends RichMapFunction[String, (String, String)] {
    val SQL = "select school from student where name = ? "
    var conn: Connection = _
    var state: PreparedStatement = _
    var rs: ResultSet = _

    override def open(parameters: Configuration): Unit = {
        conn = MySQLUtils.getConnection
        state = conn.prepareStatement(SQL)
        println("=============11111======================")
    }

    override def close(): Unit = {
        MySQLUtils.close(conn, state, rs)
        println("-------------11111----------------------")
    }


    override def map(value: String): (String, String) = {
        state.setString(1, value)
        var school = ""
        rs = state.executeQuery()
        while (rs.next()) {
            school = rs.getString(1)
        }
        (value, school)
    }
}


class MyRichFilterFunction(input: String) extends RichFilterFunction[String] {
    val SQL = "select school from student where name = ? "
    var conn: Connection = _
    var state: PreparedStatement = _
    var rs: ResultSet = _

    override def open(parameters: Configuration): Unit = {
        conn = MySQLUtils.getConnection
        state = conn.prepareStatement(SQL)
        println("=============22222======================")
    }

    override def close(): Unit = {
        MySQLUtils.close(conn, state, rs)
        println("-------------2222----------------------")
    }

    override def filter(value: String): Boolean = {
        state.setString(1, value)
        var school = ""
        rs = state.executeQuery()
        while (rs.next()) {
            school = rs.getString(1)
        }
        school == input
    }
}
