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

package me.jrwang.aloha.scheduler.master

import scala.collection.mutable

import me.jrwang.aloha.common.util.Utils
import me.jrwang.aloha.rpc.RpcEndpointRef

private[aloha] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val cores: Int,
    val memory: Int,
    val endpoint: RpcEndpointRef)
  extends Serializable {

  Utils.checkHost(host)
  assert (port > 0)

  @transient var apps: mutable.HashMap[String, ApplicationInfo] = _ // driverId => info
  @transient var state: WorkerState.Value = _
  @transient var coresUsed: Int = _
  @transient var memoryUsed: Int = _
  @transient var lastHeartBeat: Long = _

  init()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    apps = new mutable.HashMap()
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartBeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def addApplication(app: ApplicationInfo) {
    apps(app.id) = app
    memoryUsed += app.desc.memory
    coresUsed += app.desc.cores
  }

  def removeApplication(app: ApplicationInfo) {
    apps -= app.id
    memoryUsed -= app.desc.memory
    coresUsed -= app.desc.cores
  }

  def setState(state: WorkerState.Value): Unit = {
    this.state = state
  }

  def isAlive(): Boolean = this.state == WorkerState.ALIVE
}