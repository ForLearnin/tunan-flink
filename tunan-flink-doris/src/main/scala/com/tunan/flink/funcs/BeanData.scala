package com.tunan.flink.funcs

import scala.beans.BeanProperty

class BeanData {
  // name
  @BeanProperty
  var name: String = _
  // score
  @BeanProperty
  var score: Int = _

  override def toString: String = {
    "{ name:" + this.name + ", score:" + this.score + "}"
  }
}

object BeanData{
  def of(name: String,score: Int) : BeanData = {
    val rowData = new BeanData()
    rowData.setName(name)
    rowData.setScore(score)
    rowData
  }
}
