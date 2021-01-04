package com.tunan.utils

import scalikejdbc.config._

object MysqlConnectPool {

  private var isInitYqdataPool=false
  def yqdataPoolSetup(): Unit ={
    if(!isInitYqdataPool){
      poolSetupAll("yqdata")
      isInitYqdataPool=true
      println("========初始化连接========")
    }
  }

  def yqdataPoolColse(): Unit ={
    poolColse("yqdata")
    println("========关闭连接========")
  }

  def poolSetupAll(prefix:String): Unit ={
    DBsWithEnv(prefix).setupAll()
  }

  def poolColse(prefix:String): Unit ={
    DBsWithEnv(prefix).closeAll()
  }

}