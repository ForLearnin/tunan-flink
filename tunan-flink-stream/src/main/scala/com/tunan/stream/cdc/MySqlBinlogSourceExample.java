package com.tunan.stream.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Tunan
 */
public class MySqlBinlogSourceExample {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("aliyun")
                .port(3306)
                .databaseList("user_profile")
                .tableList("user_profile.cdc_order")
                .username("root")
                .password("Juan970907!@#")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000);

        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4)
                .print()
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
