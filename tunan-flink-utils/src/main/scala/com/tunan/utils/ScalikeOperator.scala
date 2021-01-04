package com.tunan.utils

import scalikejdbc._
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

class ScalikeOperator extends Serializable{


    def getConnection(name:String= "a"):Unit = {
        DBs.setup() //初始化配置
        println("=======================连接连接成功" + Thread.currentThread().getId)
    }

    def closeConnection(name:String= "a"){
        DBs.close() //初始化配置
        println("=======================连接关闭成功" + Thread.currentThread().getId)
    }

    def batchInsert( sqlStr:String,params:ListBuffer[Seq[Any]] ): Unit ={
        DB localTx { implicit session =>
            sql"$sqlStr".batch(params: _*).apply()
        }
    }

    def batchInsert( sql:String,params:Seq[Seq[Any]] ): Unit ={
        DB localTx { implicit session =>
            SQL(sql).batch(params: _*).apply()
        }
    }

    def batchByNameInsert( sql:SQL[Nothing, NoExtractor],params:Seq[Seq[(Symbol, Any)]] ): Unit ={
        DB localTx { implicit session =>
            sql.batchByName(params: _*).apply()
        }
    }

    def queryAll[A](sql:SQL[Nothing, NoExtractor],f: WrappedResultSet => A) ={
        DB readOnly { implicit session=>
            sql.map(f).list().apply()
        }
    }
}
