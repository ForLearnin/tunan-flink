package com.tunan.flink.hbase

import com.tunan.utils.HBaseUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopOutputFormat
import org.apache.hadoop.hbase.client.{Connection, Mutation, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job

object HBaseSinkApp {

    val HOST = "aliyun"
    val PORT = "2181"
    val TABLE_NAME = "student"

    def convertToHBase(input: DataSet[(String, String, Int, String)]): DataSet[(Text, Mutation)] = {
        input.map(new RichMapFunction[(String, String, Int, String), (Text, Mutation)] {
            override def map(value: (String, String, Int, String)): (Text, Mutation) = {
                val cf = "cf".getBytes()
                val id = value._1
                val name = value._2
                val age = value._3.toString
                val city = value._4

                val text = new Text(id)
                val put = new Put(id.getBytes())
                if (StringUtils.isNotEmpty(name)) {
                    put.addColumn(cf, "name".getBytes(), name.getBytes())
                }
                if (StringUtils.isNotEmpty(age + "")) {
                    put.addColumn(cf, "age".getBytes(), age.getBytes())
                }
                if (StringUtils.isNotEmpty(city)) {
                    put.addColumn(cf, "city".getBytes(), city.getBytes())
                }
                (text, put)
            }
        })
    }

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment


        val students = for {
            i <- 1 to 10
        } yield (i + "", s"name_${i}", 30 + i, s"city_${i}")

        val input = env.fromCollection(students)
        val result = convertToHBase(input)

        var connect:Connection = null

        try {
            connect = HBaseUtil.getConnection(HOST, PORT, TABLE_NAME)
            val config = connect.getConfiguration
            val job = Job.getInstance(config)
            result.output(new HadoopOutputFormat[Text, Mutation](new TableOutputFormat[Text], job))
        } catch {
            case ex:Exception => ex.printStackTrace()
         } finally {
            HBaseUtil.closeConnection(connect)
        }

        env.execute(this.getClass.getSimpleName)
    }
}