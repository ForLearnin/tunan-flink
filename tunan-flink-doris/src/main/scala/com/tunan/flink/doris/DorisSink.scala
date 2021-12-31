package com.tunan.flink.doris

import com.starrocks.connector.flink.StarRocksSink
import com.starrocks.connector.flink.row.StarRocksSinkRowBuilder
import com.starrocks.connector.flink.table.StarRocksSinkOptions
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, TableSchema}

/**
 * @Auther: 李沅芮
 * @Date: 2021/12/31 16:58
 * @Description:
 */
object DorisSink {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.fromElements(RowData(99, "stephen"), RowData(100, "lebron"))
            .addSink(StarRocksSink.sink(
                TableSchema.builder()
                    .field("score", DataTypes.INT())
                    .field("name", DataTypes.VARCHAR(20))
                    .build(),
                StarRocksSinkOptions.builder()
                    .withProperty("jdbc-url", "jdbc:mysql://master1:9030/starrocks_demo")
                    .withProperty("load-url", "master1:8030")
                    .withProperty("username", "root")
                    .withProperty("password", "")
                    .withProperty("table-name", "demo2_flink_tb1")
                    .withProperty("database-name", "starrocks_demo")
                    .withProperty("sink.properties.row_delimiter", "\\x02") // in case of raw data contains common delimiter like '\n'
                    .withProperty("sink.properties.column_separator", "\\x01") // in case of raw data contains common separator like '\t'
                    .withProperty("sink.buffer-flush.interval-ms", "5000")
                    .build(),

                // set the slots with streamRowData
                new StarRocksSinkRowBuilder[RowData]() {
                    @Override
                    def accept(slots: Array[Object], streamRowData: RowData) {
                        slots(0) = streamRowData.name
                        slots(1) = Int.box(streamRowData.score)
                    }
                }
            )).setParallelism(1)

        env.execute("StarRocksSink_BeanData")

    }

    case class RowData(score: Int, name: String)
}
