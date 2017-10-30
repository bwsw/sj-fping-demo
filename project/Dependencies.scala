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

import sbt._

object Dependencies {

  object Versions {
    val scala = "2.12.1"
  }

  lazy val sjFpingDependencies = Def.setting(Seq(
    "com.bwsw" %% "sj-engine-core" % "1.0-SNAPSHOT" % "provided",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "com.bwsw" %% "sj-engine-simulators" % "1.0-SNAPSHOT" % "test",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  ))
}
