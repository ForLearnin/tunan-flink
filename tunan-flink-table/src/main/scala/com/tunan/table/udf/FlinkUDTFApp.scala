package com.tunan.table.udf

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object FlinkUDTFApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val tableEnv = StreamTableEnvironment.create(env)
        val stream: DataStream[String] = env.socketTextStream("aliyun", 9999)

        tableEnv.createTemporaryView("word_t", stream, 'word)

        tableEnv.registerFunction("split_word", new SplitWord("-"))

        val resultTable = tableEnv.sqlQuery(
            """
              |select
              |word,words,cnt
              |from word_t,
              |lateral table(split_word(word)) as t(words,cnt)
              |""".stripMargin)

        tableEnv.toRetractStream[Row](resultTable).print()


        env.execute(this.getClass.getSimpleName)
    }
}

class SplitWord(separator: String) extends TableFunction[Row] {

    def this() {
        this(",")
    }

    // 不仅把字符串纵向炸开了，还可以横向增加字段
    def eval(words: String): Unit = {
        words.split(separator).foreach(x => {
            val row = new Row(2)
            row.setField(0, x)
            row.setField(1, 1)
            collect(row)
        })
    }

    override def getResultType: TypeInformation[Row] = {
        Types.ROW(Types.STRING, Types.INT)
    }
}

case class WordCount(words: String, cnt: Int)
