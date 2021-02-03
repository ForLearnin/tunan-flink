package com.tunan.stream.join

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.tunan.utils.MySQLPoolUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class SchoolStream extends RichParallelSourceFunction[School]{

    var SQL = "select * from study.school"

    var conn:Connection = _
    var state :PreparedStatement =_
    var rs: ResultSet = _

    var running = true

    override def open(parameters: Configuration): Unit = {
        conn = MySQLPoolUtils.getConnection
        state = conn.prepareStatement(SQL)
    }

    override def close(): Unit = {
        MySQLPoolUtils.close(rs,state,conn)

    }

    override def run(ctx: SourceFunction.SourceContext[School]): Unit = {
        rs = state.executeQuery()

        var id:Int = 0
        var name:String = ""
        var time:Long = 0L

        while(rs.next()){
            id = rs.getInt("id")
            name = rs.getString("name")
            time = rs.getLong("time")

            ctx.collect(School(id,name,time))
        }
    }

    override def cancel(): Unit = {
        running = false
    }
}
