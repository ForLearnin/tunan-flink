package com.tunan.stream.bean

case class Maps(
                 user: String,
                 longitude: Double,
                 latitude: Double,
                 province: String = "",
                 city: String = "",
                 district: String = ""
               )
