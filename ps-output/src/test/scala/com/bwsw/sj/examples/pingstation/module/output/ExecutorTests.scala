package com.bwsw.sj.examples.pingstation.module.output

import com.bwsw.sj.engine.core.environment.OutputEnvironmentManager
import com.bwsw.sj.engine.core.simulation.{EsRequestBuilder, OutputEngineSimulator}
import com.bwsw.sj.examples.pingstation.module.output.data.PingMetrics._
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try
import scala.util.parsing.json.JSON

/**
  * @author Pavel Tomskikh
  */
class ExecutorTests extends FlatSpec with Matchers with MockitoSugar {

  JSON.globalNumberParser = { value: String =>
    Try(value.toLong).getOrElse(value.toDouble)
  }

  val transactionField = "txn"
  val manager: OutputEnvironmentManager = mock[OutputEnvironmentManager]
  when(manager.isCheckpointInitiated).thenReturn(false)
  val executor = new Executor(manager)
  val requestBuilder = new EsRequestBuilder(executor.getOutputEntity)

  "Executor" should "work properly before first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)

    val transactions = Seq(
      Seq(
        Data(1497843000, "11.11.11.11", 0.5, 5, 15, 20),
        Data(1497843100, "22.22.22.22", 0.6, 6, 16, 22)),
      Seq(
        Data(1497843200, "33.33.33.33", 0.7, 7, 17, 24)))

    val exceptedQueriesData = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(_.toString))
      transactionId +: transaction.map(data => (transactionId, data))
    }

    val queries = engineSimulator.process()
    val parsedQueries = queries.map { request =>
      JSON.parseFull(request).get.asInstanceOf[Map[String, Any]]
    }
    exceptedQueriesData.length shouldBe parsedQueries.length

    exceptedQueriesData.zip(parsedQueries).foreach {
      case (transaction: Long, request: Map[String, Any]) =>
        val matchField = request("match")
        matchField shouldBe a[Map[_, _]]

        val txnField = matchField.asInstanceOf[Map[String, Any]](transactionField)
        txnField shouldBe a[Map[_, _]]

        val value = txnField.asInstanceOf[Map[String, Any]]("query")
        value shouldBe transaction

      case ((transactionId: Long, entity: Data), request: Map[String, Any]) =>
        entity.toMap(transactionId) shouldBe request

      case _ =>
        throw new IllegalStateException
    }
  }

  it should "work properly after first checkpoint" in {
    val engineSimulator = new OutputEngineSimulator(executor, requestBuilder, manager)
    // "perform" first checkpoint
    engineSimulator.wasFirstCheckpoint = true

    val transactions = Seq(
      Seq(
        Data(1497843300, "44.44.44.44", 0.8, 8, 18, 26)),
      Seq(
        Data(1497843400, "55.55.55.55", 0.9, 9, 19, 28),
        Data(1497843500, "66.66.66.66", 1.0, 10, 20, 30)))

    val exceptedQueriesData = transactions.flatMap { transaction =>
      val transactionId = engineSimulator.prepare(transaction.map(_.toString))
      transaction.map(data => (transactionId, data))
    }

    val queries = engineSimulator.process()
    val parsedQueries = queries.map { request =>
      JSON.parseFull(request).get.asInstanceOf[Map[String, Any]]
    }
    exceptedQueriesData.length shouldBe parsedQueries.length

    exceptedQueriesData.zip(parsedQueries).foreach {
      case ((transactionId, entity), request) =>
        entity.toMap(transactionId) shouldBe request
    }
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

    def toMap(transaction: Long): Map[String, Any] = Map(
      transactionField -> transaction,
      tsField -> ts,
      ipField -> ip,
      avgTimeField -> avgTime,
      totalOkField -> totalOk,
      totalUnreachableField -> totalUnreachable,
      totalField -> total)
  }

}
