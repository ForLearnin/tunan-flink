package com.tunan.table.udf

import java.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.{FunctionContext, FunctionRequirement, ScalarFunction}
object FlinkUDFApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val tableEnv = StreamTableEnvironment.create(env)
        val stream = env.socketTextStream("aliyun", 9999)






    }
}

class IPParse extends ScalarFunction{

//    var ipParse:IPUtils = _

    override def open(context: FunctionContext): Unit = {

    }

    def eval(ip: String) = {

    }

    override def getRequirements: util.Set[FunctionRequirement] = {

        null
    }

    override def close(): Unit = {

    }
}
