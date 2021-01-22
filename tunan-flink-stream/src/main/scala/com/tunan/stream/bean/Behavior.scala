package com.tunan.stream.bean

case class Behavior(userId:String,itemId:String,goodsId:String,behavior:String,timestamp:Long)


case class ItemCount(itemId:String, behavior:String, start:Long, end:Long, count:Long)