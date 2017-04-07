package com.bwsw.sj.examples.pingstation.module.regular.entities

import com.bwsw.sj.engine.core.entities.EchoResponse

/**
 * Class is in charge of collecting statistics of fping command
 *
 * @param lastTimeStamp Last timestamp of the collected responses
 * @param totalAmount Total amount of successful responses
 * @param totalTime Sum of average time of fping response
 */

case class PingEchoState(lastTimeStamp: Long, totalAmount: Long, totalTime: Double) {
  def += (echoResponse: EchoResponse): PingEchoState = {
    PingEchoState(echoResponse.ts, this.totalAmount + 1, this.totalTime + echoResponse.time)
  }
}
