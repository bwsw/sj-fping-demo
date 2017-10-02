package com.bwsw.sj.examples.pingstation.module.regular

import com.bwsw.common.{AvroSerializer, JsonSerializer}
import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.environment.ModuleEnvironmentManager
import com.bwsw.sj.common.engine.core.regular.RegularStreamingExecutor
import com.bwsw.sj.examples.pingstation.module.regular.OptionsLiterals._
import com.bwsw.sj.examples.pingstation.module.regular.entities._
import com.typesafe.scalalogging.Logger
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.util.{Failure, Try}


class Executor(manager: ModuleEnvironmentManager) extends RegularStreamingExecutor[Record](manager) {

  import StreamNames._

  private val logger = Logger(this.getClass)
  private val state = manager.getState

  private val jsonSerializer = new JsonSerializer
  private val mapOptions = jsonSerializer.deserialize[Map[String, Any]](manager.options)
  private val schemaJson = jsonSerializer.serialize(mapOptions(schemaField))
  private val parser = new Schema.Parser()
  private val schema = parser.parse(schemaJson)
  private val avroSerializer = new AvroSerializer

  override def onInit(): Unit = {
    super.onInit()
    println("OnInit")
  }

  override def onTimer(jitter: Long): Unit = {
    super.onTimer(jitter)
    println("OnTimer")
  }

  override def onIdle(): Unit = {
    super.onIdle()
    println("OnIdle")
  }

  override def onMessage(envelope: TStreamEnvelope[Record]): Unit = {
    logger.debug("Received envelope with following consumer: " + envelope.consumerName)

    val maybePingResponse = envelope.stream match {
      case `echoResponseStream` =>
        Try {
          envelope.data.toList.map { data =>
            EchoResponse(data.get(FieldNames.timestamp).asInstanceOf[Long],
              data.get(FieldNames.ip).asInstanceOf[Utf8].toString,
              data.get(FieldNames.latency).asInstanceOf[Double])
          }
        }

      case `unreachableResponseStream` =>
        Try {
          envelope.data.toList.map { data =>
            UnreachableResponse(data.get(FieldNames.timestamp).asInstanceOf[Long],
              data.get(FieldNames.ip).asInstanceOf[Utf8].toString)
          }
        }

      case stream =>
        logger.debug("Received envelope has incorrect stream field: " + stream)
        Failure(throw new Exception)
    }

    val pingResponses = maybePingResponse.get

    logger.debug("Parsed envelope to valid PingResponses: " + pingResponses.mkString(", "))

    pingResponses.foreach { pingResponse =>
      if (state.isExist(pingResponse.ip)) {
        val pingEchoState = state.get(pingResponse.ip).asInstanceOf[PingState]
        state.set(pingResponse.ip, pingEchoState + pingResponse)
      } else {
        state.set(pingResponse.ip, PingState() + pingResponse)
      }
    }
  }

  override def onBeforeCheckpoint(): Unit = {
    logger.debug("Before checkpoint: send accumulated data to output stream")

    val outputName = manager.getStreamsByTags(Array("echo", "output")).head
    val output = manager.getRoundRobinOutput(outputName)

    state.getAll.foreach {
      case (ip, pingState: PingState) =>
        output.put(pingState.getSummary(ip))

      case _ =>
        throw new IllegalStateException
    }

    state.clear
  }

  override def deserialize(bytes: Array[Byte]): GenericRecord = avroSerializer.deserialize(bytes, schema)
}

object StreamNames {
  val unreachableResponseStream = "unreachable-response"
  val echoResponseStream = "echo-response"
}