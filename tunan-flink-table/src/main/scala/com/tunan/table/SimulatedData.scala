package com.tunan.table

import java.util.Date

import com.tunan.utils.ScalikeOperator

import scala.collection.mutable.ListBuffer


object SimulatedData {

	//	order_id INT,
	//	order_date TIMESTAMP(0),
	//	customer_name STRING,
	//	price DECIMAL(10, 5),
	//	product_id INT,
	//	order_status BOOLEAN,

	val order_id = ""
	val order_date = new Date()
	val customer_name = "李四"
	val price = 99
	val product_ids = Array(101, 102, 103, 105, 105, 106, 107, 108, 109)
	val order_status = Array(0, 1)


	def main(args: Array[String]): Unit = {

		val sql = "insert into orders(order_id,order_date,customer_name,price,product_id,order_status) values(?,?,?,?,?,?)"


		var seq = ListBuffer[Seq[Any]]()
		val operator = new ScalikeOperator()

		for (a <- 1 to 10000) {
			seq += Seq(a, new Date(), "张三", 99.9, product_ids(a % 9), order_status(a % 2))
			if(a % 1000 == 0){
				operator.batchInsert(sql, seq)
				seq.clear()
			}
		}
		operator.batchInsert(sql, seq)
	}
}