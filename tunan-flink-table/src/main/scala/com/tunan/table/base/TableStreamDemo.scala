package com.tunan.table.base

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}

/**
 * @Auther: 李沅芮
 * @Date: 2022/5/13 09:06
 * @Description:
 *
 * https://www.mail-archive.com/user-zh@flink.apache.org/msg10658.html
 *
 *
 * flink run-application -t yarn-application \
 * -Djobmanager.memory.process.size=1024m \
 * -Dtaskmanager.memory.process.size=2048m \
 * -Dtaskmanager.numberOfTaskSlots=1 \
 * -Dparallelism.default=1 \
 * -Dyarn.application.name="TableStreamDemo" \
 * -c com.tunan.table.base.TableStreamDemo file:///root/jar/original-tunan-flink-table-1.0.0.jar
 */
object TableStreamDemo {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(3000)
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1500)
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig.setCheckpointTimeout(60000)
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.setParallelism(1)

        val Settings = EnvironmentSettings.newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build()

        val tableEnv = StreamTableEnvironment.create(env, Settings)
        tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)

        val sourceDDL =
            """
              |CREATE TABLE products (
              |id INT,
              |name STRING,
              |description STRING,
              |PRIMARY KEY (id) NOT ENFORCED
              |) WITH (
              |'connector' = 'mysql-cdc',
              |'hostname' = 'aliyun',
              |'port' = '3306',
              |'username' = 'root',
              |'password' = 'Juan970907!@#',
              |'database-name' = 'mydb',
              |'table-name' = 'products'
              |)
              |""".stripMargin


        val sinkDDL =
            """
              |CREATE TABLE products2 (
              |id INT,
              |name STRING,
              |description STRING,
              |PRIMARY KEY (id) NOT ENFORCED
              |) WITH (
              |'connector' = 'jdbc',
              |'driver' = 'com.mysql.cj.jdbc.Driver',
              |'url' = 'jdbc:mysql://aliyun:3306/mydb?serverTimezone=UTC&useSSL=false',
              |'username' = 'root',
              |'password' = 'Juan970907!@#',
              |'table-name' = 'products2'
              |)
              |""".stripMargin


        val transformDmlSQL = "insert into products2 select * from products"

        tableEnv.executeSql(sourceDDL)
        tableEnv.executeSql(sinkDDL)
        tableEnv.executeSql(transformDmlSQL)
    }
}