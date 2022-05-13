package com.tunan.table.base

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/*
    批处理
 */
object DatasetTableSQLApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        val tableEnv = BatchTableEnvironment.create(env)

        val file = env.readTextFile("tunan-flink-table/data/access.txt")

        val batch = file.map(row => {
            val splits = row.split(",").map(_.trim)
            Access(splits(0), splits(1), splits(2).toLong)
        })

        val sourceTable = tableEnv.fromDataSet(batch)
        //        tableEnv.createTemporaryView("tmp_table", sourceTable)
        //        val resultTable = tableEnv.sqlQuery("select * from tmp_table")
        //        tableEnv.toDataSet[Row](resultTable).print()

        val resultTable = sourceTable.groupBy("domain")
            .aggregate("sum(traffic) as traffics")
            .select("domain,traffics")
            .orderBy("traffics.desc")

        tableEnv.toDataSet[Row](resultTable).print()

    }
}

case class Access(time: String, domain: String, traffic: Long)