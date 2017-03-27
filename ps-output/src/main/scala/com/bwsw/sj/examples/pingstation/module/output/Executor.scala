package com.bwsw.sj.examples.pingstation.module.output

import java.util.Date

import com.bwsw.common.{JsonSerializer, ObjectSerializer}
import com.bwsw.sj.engine.core.entities.{OutputEnvelope, TStreamEnvelope}
import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.OutputStreamingExecutor
import com.bwsw.sj.engine.core.output.types.es._
import com.bwsw.sj.examples.pingstation.module.output.data.PingMetrics

/**
 * Handler for work with performance metrics t-stream envelopes
 *
 * Created: 23/06/2016
 *
 * @author Kseniya Mikhaleva
 */
class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[String](manager) {
  val jsonSerializer = new JsonSerializer()
  val objectSerializer = new ObjectSerializer()

  /**
   * Transform t-stream transaction to output entities
   *
   * @param envelope Input T-Stream envelope
   * @return List of output envelopes
   */
  override def onMessage(envelope: TStreamEnvelope[String]): List[OutputEnvelope] = {
    val list = envelope.data.map { s =>
      val data = new PingMetrics()
      val rawData = s.split(",")
      data.ts = new Date(rawData(0).toLong)
      data.ip = rawData(1)
      data.avgTime = rawData(2).toDouble
      data.totalOk = rawData(3).toLong
      data.totalUnreachable = rawData(4).toLong
      data.total = data.totalOk + data.totalUnreachable
      data
    }
    list
  }

  override def getOutputEntity = {
    val entityBuilder = new ElasticsearchEntityBuilder()
    val entity = entityBuilder
      .field(new DateField("ts"))
      .field(new JavaStringField("ip"))
      .field(new DoubleField("avg-time"))
      .field(new LongField("total-ok"))
      .field(new LongField("total-unreachable"))
      .field(new LongField("total"))
      .build()
    entity
  }
}

