package com.tunan.stream.state

import java.util
import java.util.Collections

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

object UserDefinedState {


	def main(args: Array[String]): Unit = {
		System.setProperty("HADOOP_USER_NAME", "hadoop")

		val env = StreamExecutionEnvironment.getExecutionEnvironment
		setCheckpoint(env)
		//      valueState(env)
		//		mapState(env)
		//		listCheckpointState(env)
		checkpointFunctionFlatMap(env)

		env.execute(this.getClass.getSimpleName)
	}

	private def setCheckpoint(env: StreamExecutionEnvironment): Unit = {
		env.enableCheckpointing(1000)
		env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 默认就是这个
		env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
		//		env.setStateBackend(new RocksDBStateBackend("file:///softwarespace/tunan-flink/tunan-flink-stream/checkpoint"))
		env.setStateBackend(new RocksDBStateBackend("file:///sparkspace/tunan-flink/tunan-flink-stream/checkpoint"))
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.seconds(2)))
	}

	def valueState(env: StreamExecutionEnvironment): DataStreamSink[(String, Double)] = {
		val socket = env.socketTextStream("aliyun", 9999).filter(_.nonEmpty).map(row => {
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
		val socket = env.socketTextStream("aliyun", 9999).filter(_.nonEmpty).map(row => {
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
		val socket = env.socketTextStream("aliyun", 9999).filter(_.nonEmpty).map(row => {
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
		val socket = env.socketTextStream("aliyun", 9999).filter(_.nonEmpty).map(row => {
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

class CheckpointFunctionFlatMap extends RichFlatMapFunction[(String, String), (String, Int)] with CheckpointedFunction {
	var checkpointedState: ListState[Int] = _
	var localCount: Int = _

	override def flatMap(value: (String, String), out: Collector[(String, Int)]): Unit = {
		localCount += 1
		out.collect(value._2, localCount)
	}

	override def snapshotState(context: FunctionSnapshotContext): Unit = {
		checkpointedState.clear()
		checkpointedState.add(localCount)
	}

	import scala.collection.JavaConverters._

	override def initializeState(context: FunctionInitializationContext): Unit = {
		checkpointedState = context.getOperatorStateStore().getListState(new ListStateDescriptor("perPartitionCount", classOf[Int]))
		for (ele <- checkpointedState.get().asScala) {
			localCount += ele
		}
	}
}


class ListCheckpointFlatMap extends RichFlatMapFunction[(String, String), (String, Int)] with ListCheckpointed[Integer] {
	private var sum: Int = 0

	override def flatMap(value: (String, String), out: Collector[(String, Int)]): Unit = {
		sum += 1
		out.collect(value._2, sum)
	}

	override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Integer] = {
		Collections.singletonList(sum)
	}

	import scala.collection.JavaConverters._

	override def restoreState(state: util.List[Integer]): Unit = {
		for (e <- state.asScala) {
			sum += e
		}
	}
}


// TODO 统计用户行为
class MapValueStateFlatMap extends RichFlatMapFunction[(String, String), (String, String, Int)] {
	private var mapState: MapState[String, Int] = _


	override def open(parameters: Configuration): Unit = {
		val mapStateDescriptor = new MapStateDescriptor[String, Int]("mapState", classOf[String], classOf[Int])
		mapState = getRuntimeContext.getMapState(mapStateDescriptor)
	}

	override def flatMap(value: (String, String), out: Collector[(String, String, Int)]): Unit = {

		var behaviorCnt = 1

		if (mapState.contains(value._2)) {
			behaviorCnt = mapState.get(value._2) + 1
		}

		mapState.put(value._2, behaviorCnt)

		out.collect((value._1, value._2, behaviorCnt))
	}

}


// TODO 每两个元素求一次平均数
class ValueStateFlatMap extends RichFlatMapFunction[(String, Int), (String, Double)]() {

	private var avgState: ValueState[(Int, Int)] = _

	override def open(parameters: Configuration): Unit = {

		val valueStateDescriptor = new ValueStateDescriptor("valueState", classOf[(Int, Int)])
		avgState = getRuntimeContext.getState(valueStateDescriptor)
	}

	override def flatMap(value: (String, Int), out: Collector[(String, Double)]): Unit = {
		val tmpState = avgState.value()

		val currentState = if (null != tmpState) {
			tmpState
		} else {
			(0, 0)
		}

		val newState = (currentState._1 + 1, currentState._2 + value._2)

		avgState.update(newState)

		if (newState._1 >= 2) {
			out.collect(value._1, newState._2 / newState._1.toDouble)
			avgState.clear()
		}
	}
}