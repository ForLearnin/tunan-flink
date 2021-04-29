package com.tunan.flink.sink


import com.tunan.utils.{FlinkKafkaSource, ScalikeOperator}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.runtime.util.ExecutorThreadFactory
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent._
import scala.collection.mutable.ListBuffer


object MySQLRichThreadSink {


    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = FlinkKafkaSource.getEnv

        val parameterTool: ParameterTool = ParameterTool.fromPropertiesFile("tunan-flink-2021/kafka-conf/parameters.properties")

        val sourceData: DataStream[String] = FlinkKafkaSource.createKafkaSource(parameterTool)

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

        sourceData.addSink(new DataUpsertSinkFunction)

        env.execute(this.getClass.getSimpleName)
    }
}

case class Student(id: Int, name: String, age: Int, sex: String, school: String, time: Long)


class DataUpsertSinkFunction extends RichSinkFunction[String] with CheckpointedFunction {
    var scalikeOperator: ScalikeOperator = _
    val bufferFlushIntervalMillis = 10000000L
    val bufferFlushMaxListSize = 3L

    private val failureThrowable = new AtomicReference[Throwable]()

    @transient private var executor: ScheduledExecutorService = _
    @transient private var scheduledFuture: ScheduledFuture[_] = _
    @transient private var numPendingRequests: AtomicLong = _

    var dataOfQueue: ConcurrentLinkedDeque[Seq[Any]] = _

    val sql = "INSERT INTO student VALUES(?,?,?,?,?,?)"

    override def open(parameters: Configuration): Unit = {
        dataOfQueue = new ConcurrentLinkedDeque[Seq[Any]]
        scalikeOperator = new ScalikeOperator

        this.executor = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("data-upsert-sink-flusher"))

        this.scheduledFuture = this.executor.scheduleWithFixedDelay(new Runnable {

            def exe = this.synchronized({
                try {
                    println("10秒时间到，触发flush")
                    flush()
                } catch {
                    case e: Exception => e.printStackTrace()
                        failureThrowable.compareAndSet(null, e)
                }
            })

            override def run(): Unit = exe
        }, bufferFlushIntervalMillis, bufferFlushIntervalMillis, TimeUnit.MILLISECONDS)

        this.numPendingRequests = new AtomicLong(0)

        scalikeOperator.getConnection()
    }

    override def invoke(value: String): Unit = {

        val next = dataOfQueue.iterator()
        while(next.hasNext){
            println(next.next())
        }


        println("===========================")
        if (value == null || value == "") {
            println(" 输入数据为空")
        }else {
            val words = value.split(",").map(_.trim)
            if(words.length == 6){
                val student = Student(words(0).toInt, words(1), words(2).toInt, words(3), words(4), words(5).toLong)

                dataAddQueue(student)

                println(numPendingRequests.get() + 1)

                if (bufferFlushMaxListSize > 0 && numPendingRequests.incrementAndGet >= bufferFlushMaxListSize) {
                    println("记录超过3条，触发flush")
                    flush()

                    numPendingRequests = new AtomicLong(0)
                }
            }else{
                println("字符长度不符合6个")
            }
        }
    }


    private def flush(): Unit = {

        println("触发flush方法")

        val dataList = new ListBuffer[Seq[Any]]

        val dataIt = dataOfQueue.iterator()
        while (dataIt.hasNext) {
            println("处理数据")
            dataList.append(dataIt.next())
            dataIt.remove()
        }

        if (dataList.size >= 0) {
            // 对插入数据的操作做容错处理，错误数据默认丢弃(异常数据)
            try {
                scalikeOperator.insertBatchData(sql, dataList)
            }catch {
                case e:Exception => e.printStackTrace()
            }
            numPendingRequests.set(0)
            dataList.clear()
        }
    }

     def dataAddQueue(student: Student): Unit = {
        dataOfQueue.add(Seq(student.id,student.name,student.age,student.sex,student.school,student.time))
    }



//    override def close(): Unit = {
//        if (this.scheduledFuture != null) {
//            this.scheduledFuture.cancel(false)
//            if (this.executor != null) this.executor.shutdownNow
//        }
//
//        scalikeOperator.closeConnection()
//    }


    // 程序挂了后重启恢复数据
    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
        while (this.numPendingRequests.get() != 0) {
            println("--------snapshotState----------")
            this.flush()
        }
    }

    override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {
    }
}