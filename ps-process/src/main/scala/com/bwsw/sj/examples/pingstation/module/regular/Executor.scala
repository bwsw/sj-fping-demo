package com.bwsw.sj.examples.pingstation.module.regular

import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.environment.ModuleEnvironmentManager
import com.bwsw.sj.common.engine.core.regular.RegularStreamingExecutor
import com.bwsw.sj.examples.pingstation.module.regular.entities._
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


class Executor(manager: ModuleEnvironmentManager) extends RegularStreamingExecutor[Record](manager) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val state = manager.getState

  override def onInit(): Unit = { super.onInit(); println("OnInit") }

  override def onTimer(jitter: Long): Unit = { super.onTimer(jitter); println("OnTimer") }

  override def onIdle(): Unit = { super.onIdle(); println("OnIdle") }

  override def onMessage(envelope: TStreamEnvelope[Record]): Unit = {
    logger.debug("Received envelope with following consumer: " + envelope.consumerName)
    println("OnMessage: " + envelope.consumerName)

    val maybePingResponse = envelope.stream match {
        case "echo-response" =>
          val data = envelope.data.head
          Try {
            EchoResponse(data.get(FieldNames.timestamp).asInstanceOf[Utf8].toString.toLong,
              data.get(FieldNames.ip).asInstanceOf[Utf8].toString,
              data.get(FieldNames.latency).asInstanceOf[Utf8].toString.toDouble)
          }

        case "unreachable-response" =>
          val data = envelope.data.head
          Try {
            UnreachableResponse(data.get(FieldNames.timestamp).asInstanceOf[Utf8].toString.toLong,
              data.get(FieldNames.ip).asInstanceOf[Utf8].toString)
          }

        case stream =>
          logger.debug("Received envelope has incorrect stream field: " + stream)
          Failure(throw new Exception)
    }

    val pingResponse = maybePingResponse match {
      case Success(pr) => pr
      case Failure(_) => return
    }

    logger.debug("Parsed envelope to valid PingResponse: " + pingResponse)

    if(state.isExist(pingResponse.ip)) {
      val pingEchoState = state.get(pingResponse.ip).asInstanceOf[PingState]
      state.set(pingResponse.ip, pingEchoState += pingResponse)
    } else {
      state.set(pingResponse.ip, PingState() += pingResponse)
    }
  }

  override def onBeforeCheckpoint(): Unit = {
    logger.debug("Before checkpoint: send accumulated data to output stream")

    val outputName = manager.getStreamsByTags(Array("echo", "output")).head
    val output = manager.getRoundRobinOutput(outputName)

    state.getAll.map(echoState => echoState._1 -> echoState._2.asInstanceOf[PingState])
      .map(unreachableState => unreachableState._2.getSummary(unreachableState._1)).foreach(output.put)

    state.clear
  }
}