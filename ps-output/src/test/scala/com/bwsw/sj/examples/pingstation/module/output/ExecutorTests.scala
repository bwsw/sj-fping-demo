package com.bwsw.sj.examples.pingstation.module.output

import com.bwsw.sj.common.dal.model.service.TStreamServiceDomain
import com.bwsw.sj.common.dal.model.stream.TStreamStreamDomain
import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.output.types.es.ElasticsearchCommandBuilder
import com.bwsw.sj.engine.core.simulation.output.{EsRequestBuilder, OutputEngineSimulator}
import com.bwsw.sj.examples.pingstation.module.output.data.PingMetrics._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for [[Executor]]
  *
  * @author Pavel Tomskikh
  */
class ExecutorTests extends FlatSpec with Matchers with MockitoSugar {

  val transactionField = "txn"
  val options = "{}"
  val outputStream = new TStreamStreamDomain("output-stream", mock[TStreamServiceDomain], 1)
  val manager: OutputEnvironmentManager = new OutputEnvironmentManager(options, Array(outputStream))
  val executor = new Executor(manager)
  val requestBuilder = new EsRequestBuilder(executor.getOutputEntity)
  val commandBuilder = new ElasticsearchCommandBuilder(transactionField, executor.getOutputEntity)

  "Executor" should "work properly before first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)

    val transactions = Seq(
      Seq(
        Data(1497843000, "11.11.11.11", 0.5, 5, 15, 20),
        Data(1497843100, "22.22.22.22", 0.6, 6, 16, 22)),
      Seq(
        Data(1497843200, "33.33.33.33", 0.7, 7, 17, 24)))

    val expectedQueries = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(_.toString))
      val deleteQuery = commandBuilder.buildDelete(transactionId)
      val insertQueries = transaction.map { data =>
        commandBuilder.buildInsert(transactionId, data.toMap)
      }

      deleteQuery +: insertQueries
    }

    val queries = engineSimulator.process()

    queries shouldBe expectedQueries
  }

  it should "work properly after first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
    // "perform" first checkpoint
    engineSimulator.beforeFirstCheckpoint = false

    val transactions = Seq(
      Seq(
        Data(1497843300, "44.44.44.44", 0.8, 8, 18, 26)),
      Seq(
        Data(1497843400, "55.55.55.55", 0.9, 9, 19, 28),
        Data(1497843500, "66.66.66.66", 1.0, 10, 20, 30)))

    val expectedQueries = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(_.toString))
      transaction.map { data =>
        commandBuilder.buildInsert(transactionId, data.toMap)
      }
    }

    val queries = engineSimulator.process()

    queries shouldBe expectedQueries
  }

  it should "throw exception if incoming data is incorrect" in {
    val incorrectData = "incorrect data"

    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
    engineSimulator.prepare(Seq(incorrectData))

    intercept[Exception] {
      engineSimulator.process()
    }
  }

  case class Data(ts: Long,
                  ip: String,
                  avgTime: Double,
                  totalOk: Long,
                  totalUnreachable: Long,
                  total: Long) {

    override def toString: String =
      s"$ts,$ip,$avgTime,$totalOk,$totalUnreachable"

    def toMap: Map[String, Any] = {
      Map(
        tsField -> ts,
        ipField -> ip,
        avgTimeField -> avgTime,
        totalOkField -> totalOk,
        totalUnreachableField -> totalUnreachable,
        totalField -> total)
    }
  }

}
