package com.tunan.item.domain

case class Access(
                       belonger_id: String,
                       member_id: String,
                       thumb: String,
                       name: String,
                       visit_time: String,
                       source_plate: String,
                       source_platform: String,
                       content: String,
                       var visit_num: Long = 0,
                       var stay_time: Long = 0,
                       var total_stay_time: Long = 0,
                       act: String,
                       title: String,
                       home_img: String,
                       house_number: String,
                       stress_name: String
                     )
