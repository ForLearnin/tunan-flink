package com.tunan.basic

import org.apache.flink.api.java.utils.ParameterTool

// 单节点配置
// 生产上 jobmanager.heap.size 要调
object ParametersApp {

	def main(args: Array[String]): Unit = {

//		fromArgs(args)
		fromPropertiesFile()
	}

	// 从args获取参数
	def fromArgs(args: Array[String]): Unit ={
		val tool = ParameterTool.fromArgs(args)
		// 必选
		val id = tool.getRequired("id")

		// 可选
		val name = tool.get("name", "暂无")

		println(id+ "\t" + name)

	}

	def fromPropertiesFile(): Unit ={
		val tool = ParameterTool.fromPropertiesFile("tunan-flink-basic/conf/parameters.properties")
		// 必选
		val id = tool.getRequired("id")

		// 可选
		val name = tool.get("name", "暂无")

		println("id: "+id+ "\t" + "name: "+name)

	}

}
