package com.tunan.stream.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.util
import java.util.Collections

object UserDefinedState {


    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "hadoop")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(3)
        setCheckpoint(env)
        //        valueState(env)
        //        mapState(env)
        //        listCheckpointState(env)
        checkpointFunctionFlatMap(env)

        env.execute(this.getClass.getSimpleName)
    }

    private def setCheckpoint(env: StreamExecutionEnvironment): Unit = {
        // 启动checkpoint
        env.enableCheckpointing(5000)
        // 精准一次的模式
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 默认就是这个
        // 保留所有检查点状态
        env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        // 设置StateBackend
        env.setStateBackend(new RocksDBStateBackend("file:///private/tunan-flink/tunan-flink-stream/checkpoint"))
        // 设置失败重试机制
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(2)))
    }

    def valueState(env: StreamExecutionEnvironment): DataStreamSink[(String, Double)] = {
        val socket = env.socketTextStream("hadoop1", 9999).filter(_.nonEmpty).map(row => {
            if (row.contains("t")) {
                throw new RuntimeException("输入 t , 拉黑程序")
            } else {
                row.toLowerCase
            }
        })
        socket.map(x => {
            val words = x.split(",")
            (words(0), words(1).toInt)
        }).keyBy(x => x._1).flatMap(new ValueStateFlatMap).print()
    }

    def mapState(env: StreamExecutionEnvironment): DataStreamSink[(String, String, Int)] = {
        val socket = env.socketTextStream("hadoop1", 9999).filter(_.nonEmpty).map(row => {
            if (row.contains("t")) {
                throw new RuntimeException("输入 t , 拉黑程序")
            } else {
                row.toLowerCase
            }
        })
        socket.map(x => {
            val words = x.split(",")
            (words(0), words(1))
        }).keyBy(x => x._1).flatMap(new MapValueStateFlatMap).print()
    }

    def listCheckpointState(env: StreamExecutionEnvironment): DataStreamSink[(String, Int)] = {
        val socket = env.socketTextStream("hadoop1", 9999).filter(_.nonEmpty).map(row => {
            if (row.contains("t")) {
                throw new RuntimeException("输入 t , 拉黑程序")
            } else {
                row.toLowerCase
            }
        })
        socket.map(x => {
            val words = x.split(",")
            (words(0), words(1))
        }).keyBy(x => x._1).flatMap(new ListCheckpointFlatMap).print()
    }

    def checkpointFunctionFlatMap(env: StreamExecutionEnvironment): DataStreamSink[(String, Int)] = {
        val socket = env.socketTextStream("hadoop1", 9999).filter(_.nonEmpty).map(row => {
            if (row.contains("t")) {
                throw new RuntimeException("输入 t , 拉黑程序")
            } else {
                row.toLowerCase
            }
        })
        socket.map(x => {
            val words = x.split(",")
            (words(0), words(1))
        }).keyBy(x => x._1).flatMap(new CheckpointFunctionFlatMap).print()
    }
}

// TODO 这里实现了CheckpointedFunction，所以定义的ListState是分区共享的
class CheckpointFunctionFlatMap extends RichFlatMapFunction[(String, String), (String, Int)] with CheckpointedFunction {
    // 定义List保存状态
    var checkpointedState: ListState[Int] = _
    // 定义当前分区内本地的状态值
    var localCount: Int = _

    override def flatMap(value: (String, String), out: Collector[(String, Int)]): Unit = {
        // 本地的状态值累加
        localCount += 1
        out.collect(value._2, localCount)
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
        // 清空List状态
        checkpointedState.clear()
        // 追加最新的状态值到List中
        checkpointedState.add(localCount)
    }

    import scala.collection.JavaConverters._

    override def initializeState(context: FunctionInitializationContext): Unit = {
        checkpointedState = context.getOperatorStateStore().getListState(new ListStateDescriptor("perPartitionCount", classOf[Int]))
        // 恢复状态值
        for (ele <- checkpointedState.get().asScala) {
            println(s"=======================================       ${ele}")
            localCount += ele
        }
    }
}


// TODO 统计行为次数，每个分区保存一个状态，这里和key无关
class ListCheckpointFlatMap extends RichFlatMapFunction[(String, String), (String, Int)] with ListCheckpointed[Integer] {
    // 分区内定义sum
    private var sum: Int = 0

    override def flatMap(value: (String, String), out: Collector[(String, Int)]): Unit = {
        // 处理分区内的没条数据状态+1
        sum += 1
        out.collect(value._2, sum)
    }

    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Integer] = {
        // 保存分区内最新的sum值
        Collections.singletonList(sum)
    }

    import scala.collection.JavaConverters._

    override def restoreState(state: util.List[Integer]): Unit = {
        // 状态恢复
        for (e <- state.asScala) {
            println(s"=======================================       ${e}")
            sum += e
        }
    }
}


// TODO 统计用户行为
class MapValueStateFlatMap extends RichFlatMapFunction[(String, String), (String, String, Int)] {
    // 定义一个Map结构的状态
    private var mapState: MapState[String, Int] = _

    override def open(parameters: Configuration): Unit = {
        // 上下文拿到状态
        val mapStateDescriptor = new MapStateDescriptor[String, Int]("mapState", classOf[String], classOf[Int])
        mapState = getRuntimeContext.getMapState(mapStateDescriptor)
    }

    override def flatMap(value: (String, String), out: Collector[(String, String, Int)]): Unit = {

        // 定义行为次数默认1
        var behaviorCnt = 1

        // 第一次是不包含的，如果包含了就是第二次以上，就+1
        if (mapState.contains(value._2)) {
            behaviorCnt = mapState.get(value._2) + 1
        }

        // 记录行为对应的次数
        mapState.put(value._2, behaviorCnt)

        // 输出主键、行为、次数
        out.collect((value._1, value._2, behaviorCnt))
    }
}


// TODO 每两个元素求一次平均数
class ValueStateFlatMap extends RichFlatMapFunction[(String, Int), (String, Double)]() {

    // 定义一个状态，分别记录次数和数值
    private var avgState: ValueState[(Int, Int)] = _

    override def open(parameters: Configuration): Unit = {

        // 上下文拿到状态
        val valueStateDescriptor = new ValueStateDescriptor("valueState", classOf[(Int, Int)])
        avgState = getRuntimeContext.getState(valueStateDescriptor)
    }

    // 使用flatmap可以才能输出collect
    override def flatMap(value: (String, Int), out: Collector[(String, Double)]): Unit = {
        // 拿到状态的值
        val tmpState = avgState.value()

        // 初始化状态值
        val currentState = if (null != tmpState) {
            tmpState
        } else {
            (0, 0)
        }

        // 累加次数
        val newState = (currentState._1 + 1, currentState._2 + value._2)

        // 更新状态
        avgState.update(newState)

        // 满足条件输出
        if (newState._1 >= 2) {
            out.collect(value._1, newState._2 / newState._1.toDouble)
            avgState.clear()
        }
    }
}