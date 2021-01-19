package com.tunan.stream.etl

import com.alibaba.fastjson.JSON
import com.tunan.stream.bean.Maps
import com.tunan.utils.HttpsUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

object MapWithHttps {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // zs,112.99286359859465,28.169224012983317
        val stream = env.socketTextStream("aliyun", 9999)

        stream.map(new RichMapFunction[String, Maps] {

            var httpClient: CloseableHttpClient = _


            override def open(parameters: Configuration): Unit = {
                httpClient = HttpClients.createDefault()
            }

            override def close(): Unit = {
                httpClient.close()
            }

            override def map(value: String): Maps = {
                val words = value.split(",")
                val user = words(0)
                val longitude = words(1).toDouble
                val latitude = words(2).toDouble

                // TODO... 去接口请求数据
                val url = s"https://restapi.amap.com/v3/geocode/regeo?output=json&key=${HttpsUtils.KEY}&location=$longitude,$latitude"
                var province = ""
                var city = ""
                var district = ""
                var response: CloseableHttpResponse = null

                try {
                    val httpGet = new HttpGet(url)
                    response = httpClient.execute(httpGet)
                    val status = response.getStatusLine.getStatusCode
                    val entity = response.getEntity
                    if (200 == status) {
                        val result = EntityUtils.toString(entity, "UTF-8")
                        val json = JSON.parseObject(result)
                        val regeocodes = json.getJSONObject("regeocode")
                        if (null != regeocodes && !regeocodes.isEmpty) {
                            val addressComponent = regeocodes.getJSONObject("addressComponent")
                            province = addressComponent.getString("province")
                            city = addressComponent.getString("city")
                            district = addressComponent.getString("district")
                        }
                    }
                }
                catch {
                    case ex: Exception => ex.printStackTrace()
                } finally {
                    response.close()
                }
                Maps(user, longitude, latitude, province, city, district)
            }
        }).print()

        env.execute(this.getClass.getSimpleName)
    }
}
