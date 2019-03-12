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

package me.jrwang.aloha.scheduler.worker

import java.io.File

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import me.jrwang.aloha.app.{Application, ApplicationDescription, ApplicationState, ExitCode}
import me.jrwang.aloha.common.{AlohaConf, Logging}
import me.jrwang.aloha.rpc.RpcEndpointRef
import me.jrwang.aloha.scheduler.ApplicationStateChanged

class AppRunner(
    val conf: AlohaConf,
    val appId: String,
    val appDesc: ApplicationDescription,
    val worker: RpcEndpointRef,
    val workerId: String,
    val host: String,
    val appDir: File,
    @volatile var state: ApplicationState.Value)
  extends Logging {
  private var workerThread: Thread = null

  private[worker] def start() {
    workerThread = new Thread(s"ApplicationRunner for $appId") {
      override def run() {
        fetchAndRunApplication()
      }
    }
    workerThread.start()
  }

  // Stop this application runner
  private[worker] def kill() {
    if (workerThread != null) {
      // the workerThread will kill the application when interrupted
      workerThread.interrupt()
      workerThread = null
      state = ApplicationState.KILLED
    }
  }

  /**
    * Run the application described in our ApplicationDescription
    */
  private def fetchAndRunApplication() {
    var app: Application = null
    try {
      app = Application.create(appDesc).withApplicationDir(appDir).withAlohaConf(conf)
      val exitStatePromise = app.start()
      state = ApplicationState.RUNNING
      worker.send(ApplicationStateChanged(appId, ApplicationState.RUNNING, None))
      val exitState = Await.result(exitStatePromise.future, Duration.Inf)
      if(exitState.code == ExitCode.FAILED) {
        worker.send(ApplicationStateChanged(appId, ApplicationState.FAILED, exitState.msg, None))
      } else {
        worker.send(ApplicationStateChanged(appId, ApplicationState.FINISHED, exitState.msg, None))
      }
    } catch {
      case _: InterruptedException =>
        logInfo(s"Runner thread for application $appId interrupted")
        state = ApplicationState.KILLED
        killApp(app, Some("User request to kill app."))
        worker.send(ApplicationStateChanged(appId, ApplicationState.KILLED, Some("User request to kill app.")))
      case e: Exception =>
        logError("Error running executor", e)
        state = ApplicationState.FAILED
        killApp(app, Some(e.toString))
        worker.send(ApplicationStateChanged(appId, ApplicationState.FAILED, Some(e.toString), Some(e)))
    } finally {
      if(app != null) {
        app.clean()
      }
    }
  }

  private def killApp(app: Application, reason: Option[String]) = {
    if(app != null) {
      try {
        app.shutdown(reason)
      } catch {
        case e: Throwable =>
          logError(s"Error while killing app $appDesc.", e)
      }
    }
  }

}