package com.tunan.stream.etl

import java.sql.Connection

import com.tunan.stream.bean.Student
import com.tunan.utils.MySQLUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object MapWithMySQL {

    val sql = "select id,name,age,sex,school from study.student where id = ?"

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val stream = env.socketTextStream("aliyun", 9999)
        stream.map(new RichMapFunction[String,Student] {
            var conn: Connection = _

            override def open(parameters: Configuration): Unit = {
                conn = MySQLUtils.getConnection
            }

            override def close(): Unit = {
                conn.close()
            }

            override def map(value: String): Student = {
                val id = value.toInt
                val pstate = conn.prepareStatement(sql)
                pstate.setInt(1,id)

                var name = ""
                var age = 0
                var sex = ""
                var school = ""
                val rs = pstate.executeQuery()
                while(rs.next()){
                    name = rs.getString("name")
                    age = rs.getInt("age")
                    sex = rs.getString("sex")
                    school = rs.getString("school")
                }
                // 由于close()不会被触发，这里的灵性关闭也许很重要
                pstate.close()
                rs.close()
                Student(id,name,age,sex,school)
            }
        }).print()


        env.execute(this.getClass.getSimpleName)
    }
}
