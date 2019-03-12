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

import java.util.Date

import me.jrwang.aloha.app.{ApplicationDescription, ApplicationState}
import me.jrwang.aloha.common.util.Utils


class ApplicationInfo(
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val startTime: Long)
  extends Serializable {

  @transient var state: ApplicationState.Value = ApplicationState.SUBMITTED
  /* If we fail when launching the app, the exception is stored here. */
  @transient var exception: Option[Exception] = None
  /* Most recent worker assigned to this app */
  @transient var worker: Option[WorkerInfo] = None

  var launched: Boolean = false

  init()

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init(): Unit = {
    state = ApplicationState.SUBMITTED
    worker = None
    exception = None
  }

  override def toString: String = {
    s"$id-${desc.name}"
  }
}