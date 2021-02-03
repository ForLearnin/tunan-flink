package com.tunan.stream.join

case class base(time:Long)

case class Student(var id: Int, var name: String, var age: Int, var sex: String, var school: String,time: Long)
case class School(var id: Int, var name: String,time: Long)


object a {
    def main(args: Array[String]): Unit = {

        println(Student(1, "", 10, "", "", 1000))
    }
}