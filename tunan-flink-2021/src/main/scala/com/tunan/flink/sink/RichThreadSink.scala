package com.tunan.flink.sink


import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.runtime.util.ExecutorThreadFactory
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import java.util.concurrent.{ConcurrentLinkedDeque, Executors, ScheduledExecutorService, ScheduledFuture}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer


object RichThreadSink {


    class DataUpsertSinkFunction extends RichSinkFunction[String] with CheckpointedFunction {
        var dataUpsertSinkDAO: DataUpsertSinkDAO = _
        val bufferFlushIntervalMillis = 3000
        val bufferFlushMaxListSize = 500

        private val failureThrowable = new AtomicReference[Throwable]()

        @transient private var executor: ScheduledExecutorService = _
        @transient private var scheduledFuture: ScheduledFuture[_] = _
        @transient private var numPendingRequests: AtomicLong = _

        var dataOfQueue: ConcurrentLinkedDeque[Seq[Any]] = _

        override def open(parameters: Configuration): Unit = {
            dataOfQueue = new ConcurrentLinkedDeque[Seq[Any]]
            dataUpsertSinkDAO = new DataUpsertSinkDAO

            this.executor = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("data-upsert-sink-flusher"))

            this.scheduledFuture = this.executor.scheduleWithFixedDelay(new Runnable {

                def exe = this.synchronized({
                    try {
                        flush()
                    } catch {
                        case e: Exception => e.printStackTrace()
                            failureThrowable.compareAndSet(null, e)
                    }
                })

                override def run(): Unit = exe
            }, bufferFlushIntervalMillis, bufferFlushIntervalMillis, TimeUnit.MILLISECONDS)

            this.numPendingRequests = new AtomicLong(0)

        }

        override def invoke(value: String): Unit = {

            if (value == null) {
                println(" 输入数据为空")
            } else {
                dataAddQueue(value)

                if (bufferFlushMaxListSize > 0 && numPendingRequests.incrementAndGet() >= bufferFlushMaxListSize) {
                    flush()

                    numPendingRequests = new AtomicLong(0)
                }
            }

        }


        def flush(): Unit = {

            val dataList = new ListBuffer[Seq[Any]]

            val dataIt = dataOfQueue.iterator()
            while (dataIt.hasNext) {
                dataList.append(dataIt.next())
                dataIt.remove()
            }

            if (dataList.size >= 0) {
                dataUpsertSinkDAO.insertDataWithBatch(dataList)
                numPendingRequests.set(0)
                dataList.clear()
            }

        }

        def dataAddQueue(value: String): Unit = {
            dataOfQueue.add(value)
        }

        override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
            if (numPendingRequests.get() != 0) {
                flush()
            }
        }

        override def initializeState(functionInitializationContext: FunctionInitializationContext): Unit = {

        }
    }


    class DataUpsertSinkDAO {

        def insertDataWithBatch(list: ListBuffer[Seq[Any]]): Unit = {
        }
    }

}
