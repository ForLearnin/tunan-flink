package com.tunan.utils

import java.util.Date

import scalikejdbc._
import scalikejdbc.config.DBs

import scala.collection.mutable.ListBuffer

class ScalikeOperator extends Serializable{

    DBs.setupAll()

    def getConnection(): Unit = {
        println("=======================连接连接成功" + Thread.currentThread().getId)
        DBs.setupAll() //初始化配置
    }

    def closeConnection(){
        println("=======================连接关闭成功" + Thread.currentThread().getId)
        DBs.closeAll() //初始化配置
    }

//    def batchInsert( sql:String,params:ListBuffer[Seq[Any]] ): Unit ={
//        DB.localTx { implicit session =>
//            SQL(sql).batch(params: _*).apply()
//        }
//    }

    def batchInsert( sql:String,params:Seq[Seq[Any]] ): Unit ={
        DB.localTx { implicit session =>
            SQL(sql).batch(params: _*).apply()
        }
    }

    def insertBatchData(sql: String, params: Seq[Seq[Any]]): Unit = {
        DB.localTx(implicit session => {
            SQL(sql).batch(params: _*).apply()
        })
    }

    def batchByNameInsert( sql:SQL[Nothing, NoExtractor],params:Seq[Seq[(Symbol, Any)]] ): Unit ={
        DB localTx { implicit session =>
            sql.batchByName(params: _*).apply()
        }
    }

    def queryAll[A](sql:SQL[Nothing, NoExtractor],f: WrappedResultSet => A): Unit ={
        DB readOnly { implicit session=>
            sql.map(f).list().apply()
        }
    }
}

object ScalikeOperator {

    def main(args: Array[String]): Unit = {

        val sql = "insert into orders(order_id,order_date,customer_name,price,product_id,order_status) values(?,?,?,?,?,?)"

        val seq = Seq(Seq("998", new Date(), "张三", 99, 101, 0))

        new ScalikeOperator().batchInsert(sql,seq)

    }
}
