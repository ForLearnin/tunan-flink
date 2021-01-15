package com.tunan.utils

import java.util.Calendar

object TimeUtils {

    //当天0点的毫秒
    def getTimeInMillis: Long = {
        val calendar: Calendar = Calendar.getInstance
        calendar.set(Calendar.HOUR_OF_DAY, 0)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        //当天0点
        val zero: Long = calendar.getTimeInMillis
        zero
    }

}
