package com.bwsw.sj.examples.pingstation.module.regular

import com.bwsw.sj.engine.core.entities._
import com.bwsw.sj.engine.core.environment.ModuleEnvironmentManager
import com.bwsw.sj.engine.core.regular.RegularStreamingExecutor
import com.bwsw.sj.examples.pingstation.module.regular.entities.PingEchoState
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


class Executor(manager: ModuleEnvironmentManager) extends RegularStreamingExecutor[Record](manager) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val state = manager.getState

  override def onMessage(envelope: TStreamEnvelope[Record]): Unit = {
    val echoResponses = envelope.data.map {
      avroRecord => Try {
        EchoResponse(avroRecord.get(EchoResponseFieldNames.timestamp).asInstanceOf[Utf8].toString.toLong,
          avroRecord.get(EchoResponseFieldNames.ip).asInstanceOf[Utf8].toString,
          avroRecord.get(EchoResponseFieldNames.latency).asInstanceOf[Utf8].toString.toDouble
        )
      } match {
        case Success(r) => r
        case Failure(_) =>
          //TODO debug msg
          null
      }
    }.filter(_ != null)

    echoResponses.foreach(echoResponse =>
      if(state.isExist(echoResponse.ip)) {
        val pingEchoState = state.get(echoResponse.ip).asInstanceOf[PingEchoState]
        state.set(echoResponse.ip, pingEchoState += echoResponse)
      } else {
        state.set(echoResponse.ip, PingEchoState(echoResponse.ts, 1, echoResponse.time))
      }
    )
  }

  override def onBeforeCheckpoint(): Unit = {
    val outputName = manager.getStreamsByTags(Array("echo", "output")).head
    val output = manager.getRoundRobinOutput(outputName) //TODO Change it

    state.getAll.map(echoState => echoState._1 -> echoState._2.asInstanceOf[PingEchoState])
      .map(echoState =>
        echoState._2.lastTimeStamp + ',' + echoState._1 + ',' + {
          if (echoState._2.totalTime != 0 && echoState._2.totalAmount != 0)
            echoState._2.totalTime / echoState._2.totalAmount
          else 0
        } + echoState._2.totalAmount
      ).foreach(output.put)

    state.clear
  }
}