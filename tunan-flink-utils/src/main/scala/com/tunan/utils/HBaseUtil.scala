package com.tunan.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

object HBaseUtil {

    def getConnection(host:String,port:String,table:String,tmpDir:String = "/tmp"):Connection = {
        val conf = HBaseConfiguration.create
        conf.set("hbase.zookeeper.quorum", host)
        conf.set("hbase.zookeeper.property.clientPort", port)
        conf.set(TableOutputFormat.OUTPUT_TABLE, table)
        //插入的时临时文件存储位置,大数据量需要
        conf.set("mapreduce.output.fileoutputformat.outputdir",tmpDir)

        val conn = ConnectionFactory.createConnection(conf)
        conn
    }

    def closeConnection(conn:Connection): Unit ={
        if(conn != null){
            conn.close()
        }
    }
}
