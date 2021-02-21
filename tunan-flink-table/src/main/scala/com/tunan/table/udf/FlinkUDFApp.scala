package com.tunan.table.udf

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row

import scala.util.Random

object FlinkUDFApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val tableEnv = StreamTableEnvironment.create(env)
        val stream: DataStream[String] = env.socketTextStream("aliyun", 9999)

        tableEnv.createTemporaryView("word", stream, 'word)

        tableEnv.registerFunction("r_word", new RandWord)

        val resultTable = tableEnv.sqlQuery("select word,r_word(word) from word")

        tableEnv.toRetractStream[Row](resultTable).print()

        env.execute(this.getClass.getSimpleName)
    }
}

class RandWord extends ScalarFunction {

    var r: Random = _


    override def open(context: FunctionContext): Unit = {
        r = Random
    }

    def eval(word: String) = {
        r.nextInt(10) + "_" + word
    }

    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
        Types.STRING
    }
}
