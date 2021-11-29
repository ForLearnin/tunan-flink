package com.tunan.flink.scala

import java.io.InputStream

/**
 * @Auther: 李沅芮
 * @Date: 2021/11/26 16:19
 * @Description:
 */
object ResourcesFile {

    def main(args: Array[String]): Unit = {
        var streamSource: InputStream = null

        // 按行读取resource下的文件
        streamSource = this.getClass.getClassLoader.getResourceAsStream("log4j.properties")
        val lines: Iterator[String] = scala.io.Source.fromInputStream(streamSource).getLines
        lines.foreach(line => {
            println(line)
        })
    }
}
