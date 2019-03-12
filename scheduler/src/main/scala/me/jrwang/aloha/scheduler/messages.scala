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

package me.jrwang.aloha.scheduler

import me.jrwang.aloha.app.{ApplicationDescription, ApplicationState}
import me.jrwang.aloha.app.ApplicationState.ApplicationState
import me.jrwang.aloha.common.util.Utils
import me.jrwang.aloha.scheduler.master.RecoveryState.MasterState
import me.jrwang.aloha.scheduler.master.{ApplicationInfo, WorkerInfo}
import me.jrwang.aloha.rpc.{RpcAddress, RpcEndpointRef}
import me.jrwang.aloha.scheduler.worker.AppDesc

/**
  * @param id the worker id
  * @param host the worker host
  * @param port the worker post
  * @param worker the worker endpoint ref
  * @param cores the core number of worker
  * @param memory the memory size of worker
  * @param masterAddress the master address used by the worker to connect
  */
case class RegisterWorker(
  id: String,
  host: String,
  port: Int,
  worker: RpcEndpointRef,
  cores: Int,
  memory: Int,
  masterAddress: RpcAddress)

case class Heartbeat(workerId: String, worker:RpcEndpointRef)

case class ReconnectWorker(masterUrl: String)

sealed trait RegisterWorkerResponse

case class RegisterWorkerFailed(message: String) extends RegisterWorkerResponse

case class RegisteredWorker(
    master: RpcEndpointRef)
  extends RegisterWorkerResponse

case object MasterInStandby extends RegisterWorkerResponse

case object ElectedLeader

case object RevokedLeadership

case object CompleteRecovery

case object CheckForWorkerTimeOut

case class RemoveWorker(worker: WorkerInfo, reason: String)

case class MasterChanged(master: RpcEndpointRef)

case class WorkerSchedulerStateResponse(id: String, apps: List[AppDesc])

/**
  * A worker will send this message to the master when it registers with the master. Then the
  * master will compare them with the apps in the master and tell the worker to
  * kill the unknown apps.
  */
case class WorkerLatestState(id: String, apps: List[AppDesc])

case object ReRegisterWithMaster

case object SendHeartBeat

case object WorkDirCleanup // Sent to Worker endpoint periodically for cleaning up app folders

case class LaunchApplication(
    masterUrl: String,
    appId: String,
    appDesc: ApplicationDescription)

case class KillApplication(masterUrl: String, appId: String)

case class ApplicationStateChanged(
  appId: String,
  state: ApplicationState.Value,
  message: Option[String],
  exception: Option[Exception] = None)

case class ApplicationFinished(id: String)

case class RequestSubmitApplication(appDescription: ApplicationDescription)

case class SubmitApplicationResponse(
    master: RpcEndpointRef,
    success: Boolean,
    applicationId: Option[String],
    message: String)

case class RequestKillApplication(appId: String)

case class KillApplicationResponse(
    master: RpcEndpointRef,
    success: Boolean,
    applicationId: String,
    message: String)

case class RequestApplicationStatus(driverId: String)

case class ApplicationStatusResponse(found: Boolean, state: Option[ApplicationState],
  workerId: Option[String], workerHostPort: Option[String], exception: Option[Exception])

case object RequestMasterState

case class MasterStateResponse(
    host: String,
    port: Int,
    restPort: Option[Int],
    workers: Array[WorkerInfo],
    activeApps: Array[ApplicationInfo],
    completedApps: Array[ApplicationInfo],
    status: MasterState) {

  Utils.checkHost(host)
  assert (port > 0)

  def uri: String = "aloha://" + host + ":" + port
  def restUri: Option[String] = restPort.map { p => "http://" + host + ":" + p }
}

case object BoundPortsRequest

case class BoundPortsResponse(rpcEndpointPort: Int, restPort: Option[Int])