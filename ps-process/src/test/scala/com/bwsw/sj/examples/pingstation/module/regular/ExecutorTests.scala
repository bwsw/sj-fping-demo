/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.sj.examples.pingstation.module.regular

import java.util.Date

import com.bwsw.common.JsonSerializer
import com.bwsw.sj.common.dal.model.service.TStreamServiceDomain
import com.bwsw.sj.common.dal.model.stream.TStreamStreamDomain
import com.bwsw.sj.common.engine.core.state.{RAMStateService, StateSaverInterface, StateStorage}
import com.bwsw.sj.engine.core.simulation.regular.RegularEngineSimulator
import com.bwsw.sj.engine.core.simulation.state._
import com.bwsw.sj.examples.pingstation.module.regular.OptionsLiterals._
import com.bwsw.sj.examples.pingstation.module.regular.StreamNames._
import com.bwsw.sj.examples.pingstation.module.regular.entities.{EchoResponse, PingResponse, PingState, UnreachableResponse}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.util.Utf8
import org.apache.avro.{Schema, SchemaBuilder}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

/**
  * Tests for [[Executor]]
  *
  * @author Pavel Tomskikh
  */
class ExecutorTests extends FlatSpec with Matchers with MockitoSugar with BeforeAndAfterEach {
  val jsonSerializer = new JsonSerializer

  val unreachableResponseSchema = SchemaBuilder.record("regex").fields()
    .name(FieldNames.ip).`type`().stringType().noDefault()
    .name(FieldNames.timestamp).`type`().longType().noDefault()
    .endRecord()

  val echoResponseSchema = SchemaBuilder.record("regex").fields()
    .name(FieldNames.ip).`type`().stringType().noDefault()
    .name(FieldNames.latency).`type`().doubleType().noDefault()
    .name(FieldNames.timestamp).`type`().longType().noDefault()
    .endRecord()

  val partitions = 3
  val outputStream = new TStreamStreamDomain(
    name = "output",
    service = mock[TStreamServiceDomain],
    partitions = partitions,
    tags = Array("echo", "output"),
    creationDate = new Date())
  val stateService = new RAMStateService(mock[StateSaverInterface], new StateLoaderMock)
  val stateStorage = new StateStorage(stateService)

  val unreachableResponses = Seq(
    UnreachableResponse(101, "10.10.10.10"),
    UnreachableResponse(102, "20.20.20.20"),
    UnreachableResponse(103, "10.10.10.10"),
    UnreachableResponse(104, "30.30.30.30"),
    UnreachableResponse(105, "30.30.30.30"),
    UnreachableResponse(106, "20.20.20.20"),
    UnreachableResponse(107, "20.20.20.20"))

  val echoResponses = Seq(
    EchoResponse(201, "10.10.10.10", 10.1),
    EchoResponse(202, "20.20.20.20", 20.1),
    EchoResponse(203, "10.10.10.10", 10.2),
    EchoResponse(204, "30.30.30.30", 30.1),
    EchoResponse(205, "20.20.20.20", 20.2),
    EchoResponse(206, "20.20.20.20", 20.3),
    EchoResponse(207, "30.30.30.30", 30.2),
    EchoResponse(208, "20.20.20.20", 20.4),
    EchoResponse(209, "30.30.30.30", 30.3))

  override def beforeEach(): Unit = stateStorage.clear()


  "Executor" should "put correct data in state (unreachable response) (without checkpoint)" in
    new SimulatorForUnreachableResponse {
      simulator.prepareTstream(unreachableResponses.map(pingResponseToRecord), unreachableResponseStream)
      val expectedResult = SimulationResult(Seq.empty, createState(unreachableResponses))

      simulator.process() shouldBe expectedResult
    }

  it should "put correct data in state (echo response) (without checkpoint)" in new SimulatorForEchoResponse {
    simulator.prepareTstream(echoResponses.map(pingResponseToRecord), echoResponseStream)
    val expectedResult = SimulationResult(Seq.empty, createState(echoResponses))

    simulator.process() shouldBe expectedResult
  }

  it should "move data before checkpoint from state to output stream (unreachable response)" in
    new SimulatorForUnreachableResponse {
      simulator.prepareState(createState(unreachableResponses))
      val expectedResult = SimulationResult(
        Seq(StreamData(outputStream.name, createOutputElements(unreachableResponses))),
        Map.empty)

      simulator.beforeCheckpoint(false) shouldBe expectedResult
    }

  it should "move data before checkpoint from state to output stream (echo response)" in new SimulatorForEchoResponse {
    simulator.prepareState(createState(echoResponses))
    val expectedResult = SimulationResult(
      Seq(StreamData(outputStream.name, createOutputElements(echoResponses))),
      Map.empty)

    simulator.beforeCheckpoint(false) shouldBe expectedResult
  }

  it should "throw exception if it got data from unknown stream (unreachable response)" in
    new SimulatorForUnreachableResponse {
      simulator.prepareTstream(unreachableResponses.map(pingResponseToRecord), "unknown-stream")

      an[Exception] shouldBe thrownBy(simulator.process())
    }

  it should "throw exception if it got data from unknown stream (echo response)" in new SimulatorForEchoResponse {
    simulator.prepareTstream(echoResponses.map(pingResponseToRecord), "unknown-stream")

    an[Exception] shouldBe thrownBy(simulator.process())
  }


  def createOptions(schema: Schema): String =
    s"""{"$schemaField":${unreachableResponseSchema.toString()}}"""

  def pingResponseToRecord(pingResponse: PingResponse) = pingResponse match {
    case UnreachableResponse(timestamp, ip) =>
      val record = new Record(unreachableResponseSchema)
      record.put(FieldNames.timestamp, timestamp)
      record.put(FieldNames.ip, new Utf8(ip))

      record

    case EchoResponse(timestamp, ip, latency) =>
      val record = new Record(echoResponseSchema)
      record.put(FieldNames.timestamp, timestamp)
      record.put(FieldNames.ip, new Utf8(ip))
      record.put(FieldNames.latency, latency)

      record
  }

  def createState(pingResponses: Seq[PingResponse]): Map[String, PingState] = {
    val state = new StateStorage(new RAMStateService(mock[StateSaverInterface], new StateLoaderMock))

    pingResponses.foreach { pingResponse =>
      val pingState = {
        if (state.isExist(pingResponse.ip))
          state.get(pingResponse.ip).asInstanceOf[PingState]
        else
          PingState()
      } + pingResponse

      state.set(pingResponse.ip, pingState)
    }

    state.getAll.mapValues(_.asInstanceOf[PingState])
  }

  def createOutputElements(pingResponses: Seq[PingResponse]): Array[PartitionData] = {
    var partition = 0
    val partitionDataArray = (0 until partitions).toArray.map(PartitionData(_))

    createState(pingResponses).foreach {
      case (ip, pingState) =>
        partitionDataArray(partition) += pingState.getSummary(ip)
        partition += 1
        if (partition == partitions) partition = 0
    }

    partitionDataArray
  }


  trait SimulatorForUnreachableResponse {
    val manager = new ModuleEnvironmentManagerMock(
      stateStorage,
      createOptions(unreachableResponseSchema),
      Array(outputStream),
      new TStreamsSenderThreadMock(Set(outputStream.name)))
    val executor = new Executor(manager)
    val simulator = new RegularEngineSimulator(executor, manager)
  }

  trait SimulatorForEchoResponse {
    val manager = new ModuleEnvironmentManagerMock(
      stateStorage,
      createOptions(echoResponseSchema),
      Array(outputStream),
      new TStreamsSenderThreadMock(Set(outputStream.name)))
    val executor = new Executor(manager)
    val simulator = new RegularEngineSimulator(executor, manager)
  }

}
