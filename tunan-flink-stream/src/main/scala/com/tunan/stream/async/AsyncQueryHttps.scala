package com.tunan.stream.async

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import com.tunan.stream.bean.Maps
import com.tunan.utils.HttpsUtils
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}
import org.apache.http.util.EntityUtils

import scala.concurrent.{ExecutionContext, Future}

/**
 * 异步请求http
 */
object AsyncQueryHttps {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val stream = env.socketTextStream("aliyun", 9999)

        // capacity 默认100   线程池 * 并行度 = ?
        val result = AsyncDataStream.unorderedWait(stream, new AsyncHttpsRequest(), 2000, TimeUnit.MILLISECONDS, 20)
        result.print()
        env.execute(this.getClass.getSimpleName)
    }
}


class AsyncHttpsRequest extends RichAsyncFunction[String, Maps] {
    implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

    var httpAsyncClient: CloseableHttpAsyncClient = _

    override def open(parameters: Configuration): Unit = {
        // 定义Request配置参数
        val config = RequestConfig.custom().setSocketTimeout(5000)
          .setConnectTimeout(5000)
          .build()

        // 拿到Async Http Client
        httpAsyncClient = HttpAsyncClients.custom().setMaxConnTotal(50)
          .setDefaultRequestConfig(config).build()

        // 启动Async Http Client
        httpAsyncClient.start()
    }

    override def close(): Unit = {
        httpAsyncClient.close()
    }

    override def asyncInvoke(value: String, resultFuture: ResultFuture[Maps]): Unit = {
        val words = value.split(",")
        val user = words(0)
        val longitude = words(1).toDouble
        val latitude = words(2).toDouble

        // TODO... 去接口请求数据
        val url = s"https://restapi.amap.com/v3/geocode/regeo?output=json&key=${HttpsUtils.KEY}&location=$longitude,$latitude"

        // 发起Get请求
        val get = new HttpGet(url)
        val future = httpAsyncClient.execute(get, null)

        val eventualTuple = Future {
            val response: HttpResponse = future.get()
            var province = ""
            var city = ""
            var district = ""

            val status = response.getStatusLine.getStatusCode
            val entity = response.getEntity
            if (200 == status) {
                val result = EntityUtils.toString(entity, "UTF-8")
                val json = JSON.parseObject(result)

                val regeocode = json.getJSONObject("regeocode")
                if (null != regeocode && !regeocode.isEmpty) {
                    val addressComponent = regeocode.getJSONObject("addressComponent")
                    province = addressComponent.getString("province")
                    city = addressComponent.getString("city")
                    district = addressComponent.getString("district")
                }
            }
            (province, city, district)
        }

        eventualTuple.onSuccess {
            case (province, city, district) => resultFuture.complete(Iterable(Maps(user, longitude, latitude, province, city, district)))
        }
    }
}