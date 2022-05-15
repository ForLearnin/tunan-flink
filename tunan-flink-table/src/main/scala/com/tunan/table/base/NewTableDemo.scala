package com.tunan.table.base

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object NewTableDemo {

	def main(args: Array[String]): Unit = {

		val settings = EnvironmentSettings
		  .newInstance()
		  .inStreamingMode()
		  .useBlinkPlanner()
		  .build()

		val tableEnv = TableEnvironment.create(settings)

		// 创建源表
		val createSourceDDL = "create table a"
		tableEnv.executeSql(createSourceDDL)

		// 创建目标表
		val createDistDDL = "create table b"
		tableEnv.executeSql(createDistDDL)

		// 执行SQL进行表的转换
		val resultTable = tableEnv.sqlQuery("select name from a")

		// 创建一张用于控制台打印输出的表
		val createPrintOutDDL = "CREATE TABLE printOutTable (" +
		  " user_name STRING, " +
		  " cnt BIGINT " +
		  ") WITH (" +
		  " 'connector' = 'print' " +
		  ")";

		// 输出表
		tableEnv.executeSql("insert into b from a")
		resultTable.executeInsert("b")

	}
}
