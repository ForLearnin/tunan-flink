package com.tunan.stream.bean


case class AccessPage(userId:String,province:String,domain:String,ts:Long)
case class CountByProvince(end:String, province:String, cnts:Long)
case class PVCount(ket:String,end:String,cnts:Long)
case class UVCount(end:String,cnts:Long)