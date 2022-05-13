package com.tunan.table.base

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object DataStreamTableSQLApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

        val stream = env.fromElements("zs,zs,zs", "ls,ls")

        val wc = stream
            .flatMap(_.split(","))
            .map(Word)

        val sourceTable = tableEnv.fromDataStream(wc)
        val resultTable = tableEnv.sqlQuery(
            s"""
               |select word,count(1) from $sourceTable
               |group by word
               |""".stripMargin)

        tableEnv
            .toRetractStream[Row](resultTable)
            .filter(_._1)
            .print()

        env.execute(this.getClass.getSimpleName)
    }
}

case class Word(word: String)
