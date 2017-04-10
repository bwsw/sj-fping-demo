package com.bwsw.sj.examples.pingstation.module.regular.entities

/**
 * Class is in charge of collecting statistics of fping command
 *
 * @param lastTimeStamp Last timestamp of the collected responses
 * @param totalTime Sum of average time of fping response
 * @param totalSuccessful Total amount of successful responses
 * @param totalUnreachable Total amount of unreachable responses
 */

case class PingState(lastTimeStamp: Long = 0, totalTime: Double = 0, totalSuccessful: Long = 0, totalUnreachable: Long = 0) {

  def += (pingResponse: PingResponse): PingState = pingResponse match {
    case er: EchoResponse => PingState(er.ts, totalTime + er.time, totalSuccessful + 1, totalUnreachable)
    case ur: UnreachableResponse => PingState(ur.ts, totalTime, totalSuccessful, totalUnreachable + 1)
  }

  def getSummary(ip: String): String = {
    lastTimeStamp + ',' + ip + ',' +
    {
      if(totalSuccessful > 0) totalTime / totalSuccessful
      else 0
    } +
    totalSuccessful + ',' + totalUnreachable
  }
}
