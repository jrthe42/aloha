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

import javax.servlet.http.HttpServletResponse

import me.jrwang.aloha.app.ApplicationDescription
import me.jrwang.aloha.common.AlohaConf
import me.jrwang.aloha.rpc.RpcEndpointRef
import me.jrwang.aloha.scheduler._


/**
  * A server that responds to requests submitted via REST api.
  * This is intended to be embedded in the standalone Master and used in cluster mode only.
  *
  * This server responds with different HTTP codes depending on the situation:
  *   200 OK - Request was processed successfully
  *   400 BAD REQUEST - Request was malformed, not successfully validated, or of unexpected type
  *   468 UNKNOWN PROTOCOL VERSION - Request specified a protocol this server does not understand
  *   500 INTERNAL SERVER ERROR - Server throws an exception internally while processing the request
  *
  * The server always includes a JSON representation of the relevant [[SubmitRestProtocolResponse]]
  * in the HTTP body. If an error occurs, however, the server will include an [[ErrorResponse]]
  * instead of the one expected by the client. If the construction of this error response itself
  * fails, the response will consist of an empty body with a response code that indicates internal
  * server error.
  *
  * @param host the address this server should bind to
  * @param requestedPort the port this server will attempt to bind to
  * @param masterConf the conf used by the Master
  * @param masterEndpoint reference to the Master endpoint to which requests can be sent
  * @param masterUrl the URL of the Master new drivers will attempt to connect to
  */
private[scheduler] class StandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterConf: AlohaConf,
    masterEndpoint: RpcEndpointRef,
    masterUrl: String)
  extends RestSubmissionServer(host, requestedPort, masterConf) {

  protected override val submitRequestServlet =
    new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
  protected override val killRequestServlet =
    new StandaloneKillRequestServlet(masterEndpoint, masterConf)
  protected override val statusRequestServlet =
    new StandaloneStatusRequestServlet(masterEndpoint, masterConf)
}

/**
  * A servlet for handling kill requests passed to the [[StandaloneRestServer]].
  */
private[rest] class StandaloneKillRequestServlet(masterEndpoint: RpcEndpointRef, conf: AlohaConf)
  extends KillRequestServlet {

  protected def handleKill(submissionId: String): KillSubmissionResponse = {
    val response = masterEndpoint.askSync[KillApplicationResponse](
      RequestKillApplication(submissionId))
    val k = new KillSubmissionResponse
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    k
  }
}

/**
  * A servlet for handling status requests passed to the [[StandaloneRestServer]].
  */
private[rest] class StandaloneStatusRequestServlet(masterEndpoint: RpcEndpointRef, conf: AlohaConf)
  extends StatusRequestServlet {

  protected def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val response = masterEndpoint.askSync[ApplicationStatusResponse](
      RequestApplicationStatus(submissionId))
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.submissionId = submissionId
    d.success = response.found
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }
}

/**
  * A servlet for handling submit requests passed to the [[StandaloneRestServer]].
  */
private[rest] class StandaloneSubmitRequestServlet(
    masterEndpoint: RpcEndpointRef,
    masterUrl: String,
    conf: AlohaConf)
  extends SubmitRequestServlet {

  /**
    * Build a application description from the fields specified in the submit request.
    */
  private def buildDriverDescription(request: CreateSubmissionRequest): ApplicationDescription = {
    val name: String = request.name
    val entryPoint: String = request.entryPoint
    val libs: Array[String] = Option(request.libs).getOrElse(Array())
    val args: String = request.args
    val memory: Int = Option(request.memory).getOrElse(1024)
    val cores: Int = Option(request.cores).getOrElse(1)
    val supervise: Boolean = Option(request.supervise).getOrElse(false)
    ApplicationDescription(name, entryPoint, libs, args, memory, cores, supervise)
  }

  /**
    * Handle the submit request and construct an appropriate response to return to the client.
    *
    * This assumes that the request message is already successfully validated.
    * If the request message is not of the expected type, return error to the client.
    */
  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val appDescription = buildDriverDescription(submitRequest)
        val response = masterEndpoint.askSync[SubmitApplicationResponse](
          RequestSubmitApplication(appDescription))
        val submitResponse = new CreateSubmissionResponse
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.applicationId.orNull
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          // If there are fields that the server does not know about, warn the client
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
}
