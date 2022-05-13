package com.tunan.table.base

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @Auther: 李沅芮
 * @Date: 2022/5/13 09:06
 * @Description:
 */
object StreamTableDemo {

    def main(args: Array[String]): Unit = {

        // 环境配置
        val settings = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()

        val tEnv: TableEnvironment = TableEnvironment.create(settings)

        tEnv.executeSql("CREATE TABLE products (\n" +
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
            "  )"
        )

        tEnv.from("products")
            .select("id, name, description")
            .execute()
            .print()

        tEnv.execute("StreamTableDemo")
    }
}