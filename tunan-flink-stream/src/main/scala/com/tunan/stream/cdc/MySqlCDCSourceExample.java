package com.tunan.stream.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Tunan
 */
public class MySqlCDCSourceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        //设置job名称
        tEnv.getConfig().getConfiguration().setString("pipeline.name", "FlinkCDCTest");

        String createSqlDwd = "CREATE TABLE products (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    description STRING,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                "  ) WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = 'aliyun',\n" +
                "    'port' = '3306',\n" +
                "    'username' = 'root',\n" +
                "    'password' = 'Juan970907!@#',\n" +
                "    'database-name' = 'mydb',\n" +
                "    'table-name' = 'products'\n" +
                "  )";
        tEnv.executeSql(createSqlDwd);

    }
}
