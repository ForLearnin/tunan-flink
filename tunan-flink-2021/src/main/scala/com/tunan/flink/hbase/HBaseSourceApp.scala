package com.tunan.flink.hbase

import com.tunan.utils.HBaseUtil
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.hadoop.hbase.client.{Connection, HTable, Result, Scan, Table}
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import scalikejdbc.NoSession.connection


object HBaseSourceApp {

    val HOST = "aliyun"
    val PORT = "2181"
    val TABLE_NAME = "student"
    val CF = "cf"


    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        env.createInput(new HBaseSource).print()

        env.execute(this.getClass.getSimpleName)
    }

    class HBaseSource extends TableInputFormat[Tuple4[String, String, Int, String]] {
        def createTable(): HTable = {

            var ht: HTable = null
            var connection: Connection = null
            try {
                connection = HBaseUtil.getConnection(HOST, PORT, TABLE_NAME)
                val conf = connection.getConfiguration
                ht = new HTable(conf,getTableName)
            } catch {
                case ex: Exception => ex.printStackTrace()
            } finally {
                HBaseUtil.closeConnection(connection)
            }
            ht
        }

        override def configure(parameters: Configuration): Unit = {
            table = createTable()
            if (table != null) {
                scan = getScanner
            }
        }

        override def getScanner: Scan = {
            val scan = new Scan()
            scan.addFamily(CF.getBytes())
            scan
        }

        override def getTableName: String = {
            TABLE_NAME
        }

        override def mapResultToTuple(result: Result): Tuple4[String, String, Int, String] = {
            new Tuple4(
                Bytes.toString(result.getRow),
                Bytes.toString(result.getValue(CF.getBytes(), "name".getBytes())),
                Bytes.toString(result.getValue(CF.getBytes(), "age".getBytes())).toInt,
                Bytes.toString(result.getValue(CF.getBytes(), "city".getBytes())
                )
            )
        }
    }

}


