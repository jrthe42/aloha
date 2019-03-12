/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.jrwang.aloha.scheduler.rest

/**
  * An abstract request sent from the client in the REST application submission protocol.
  */
private[rest] abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  protected override def doValidate(): Unit = {
    super.doValidate()
  }
}

/**
  * A request to launch a new application in the REST application submission protocol.
  *
  * case class ApplicationDescription(
  *   name: String,
  *   entryPoint: String,
  *   libs: Array[String],
  *   args: String,
  *   memory: Int,
  *   cores: Int = 1,
  *   supervise: Boolean = false
  * )
  */
private[rest] class CreateSubmissionRequest extends SubmitRestProtocolRequest {
  var name: String = null
  var entryPoint: String = null
  var libs: Array[String] = null
  var args: String = null
  var memory: Int = 1024
  var cores: Int = 1
  var supervise: Boolean = false

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(name, "name")
    assertFieldIsSet(entryPoint, "entryPoint")
  }
}
