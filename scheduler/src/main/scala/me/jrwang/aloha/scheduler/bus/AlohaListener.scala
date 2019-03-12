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

package me.jrwang.aloha.scheduler.bus

import me.jrwang.aloha.app.ApplicationState
import me.jrwang.aloha.common.Logging

trait AlohaEvent {}

case class AppStateChangedEvent(
    appId: String,
    state: ApplicationState.Value,
    msg: Option[String])
  extends AlohaEvent

case class AppRelaunchedEvent(
    oldAppId: String,
    newAppId: String,
    msg: Option[String])
  extends AlohaEvent

/**
  * Interface for listening to events from the Aloha scheduler.
  *
  */
trait AlohaEventListener {
  def onApplicationStateChange(event: AppStateChangedEvent): Unit

  def onApplicationRelaunched(event: AppRelaunchedEvent): Unit

  def onOtherEvent(event: AlohaEvent): Unit
}

/**
  * A default implementation for `AlohaEventListener` that has no-op implementations for
  * * all callbacks.
 */
class NopAlohaListener extends AlohaEventListener with Logging {
  override def onApplicationStateChange(event: AppStateChangedEvent): Unit = {
    logInfo(s"Receive event $event.")
  }

  override def onApplicationRelaunched(event: AppRelaunchedEvent): Unit = {
    logInfo(s"Receive event $event.")
  }

  override def onOtherEvent(event: AlohaEvent): Unit = {
    logInfo(s"Receive event $event.")
  }
}