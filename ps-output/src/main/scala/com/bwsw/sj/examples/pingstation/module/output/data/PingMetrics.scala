package com.bwsw.sj.examples.pingstation.module.output.data

import java.util.Date

import com.bwsw.sj.engine.core.entities.OutputEnvelope

/**
  * Created: 23/06/2016
  *
  * @author Kseniya Mikhaleva
  */
class PingMetrics extends OutputEnvelope {

  import PingMetrics._

  var ts: Date = null
  var ip: String = null
  var avgTime: Double = 0
  var totalOk: Long = 0
  var totalUnreachable: Long = 0
  var total: Long = 0

  override def getFieldsValue = {
    Map(
      tsField -> ts,
      ipField -> ip,
      avgTimeField -> avgTime,
      totalOkField -> totalOk,
      totalUnreachableField -> totalUnreachable,
      totalField -> total
    )
  }
}

object PingMetrics {
  val tsField = "ts"
  val ipField = "ip"
  val avgTimeField = "avg-time"
  val totalOkField = "total-ok"
  val totalUnreachableField = "total-unreachable"
  val totalField = "total"
}
