package com.bwsw.sj.examples.pingstation.module.output.data

import java.util.Date

import com.bwsw.sj.engine.core.entities.OutputEnvelope
import com.fasterxml.jackson.annotation.JsonProperty

/**
  * Created: 23/06/2016
  *
  * @author Kseniya Mikhaleva
  */
class PingMetrics extends OutputEnvelope {
  var ts: Date = null
  var ip: String = null
  var avgTime: Double= 0
  var totalOk: Long= 0
  var totalUnreachable: Long= 0
  var total: Long = 0

  override def getFieldsValue = {
    Map(
      "ts" -> ts,
      "ip" -> ip,
      "avg-time" -> avgTime,
      "total-ok" -> totalOk,
      "total-unreachable" -> totalUnreachable,
      "total" -> total
    )
  }
}