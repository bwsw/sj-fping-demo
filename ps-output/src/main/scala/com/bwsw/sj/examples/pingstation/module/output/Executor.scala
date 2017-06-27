package com.bwsw.sj.examples.pingstation.module.output

import java.util.Date

import com.bwsw.sj.common.engine.core.entities.TStreamEnvelope
import com.bwsw.sj.common.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.types.es._
import com.bwsw.sj.common.engine.core.output.{Entity, OutputStreamingExecutor}
import com.bwsw.sj.examples.pingstation.module.output.data.PingMetrics
import com.bwsw.sj.examples.pingstation.module.output.data.PingMetrics._

import scala.collection.mutable

class Executor(manager: OutputEnvironmentManager) extends OutputStreamingExecutor[String](manager) {

  /**
    * Transform t-stream transaction to output entities
    *
    * @param envelope Input T-Stream envelope
    * @return List of output envelopes
    */
  override def onMessage(envelope: TStreamEnvelope[String]): mutable.Queue[PingMetrics] = {
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

  override def getOutputEntity: Entity[String] = {
    val entityBuilder = new ElasticsearchEntityBuilder()
    val entity = entityBuilder
      .field(new DateField(tsField))
      .field(new JavaStringField(ipField))
      .field(new DoubleField(avgTimeField))
      .field(new LongField(totalOkField))
      .field(new LongField(totalUnreachableField))
      .field(new LongField(totalField))
      .build()
    entity
  }
}
