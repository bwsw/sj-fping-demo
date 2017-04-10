package com.bwsw.sj.examples.pingstation.module.regular.entities

abstract class PingResponse {
  val ts: Long
  val ip: String
}

case class EchoResponse(ts: Long, ip: String, time: Double) extends PingResponse

case class UnreachableResponse(ts: Long, ip: String) extends PingResponse