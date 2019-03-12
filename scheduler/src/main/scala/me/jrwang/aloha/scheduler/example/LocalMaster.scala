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

package me.jrwang.aloha.scheduler.example

import me.jrwang.aloha.app.ApplicationDescription
import me.jrwang.aloha.common.AlohaConf
import me.jrwang.aloha.rpc.RpcEnv
import me.jrwang.aloha.scheduler.{BoundPortsRequest, BoundPortsResponse, RequestSubmitApplication, SubmitApplicationResponse}
import me.jrwang.aloha.scheduler.master.Master
import me.jrwang.aloha.scheduler._

object LocalMaster {
  val SYSTEM_NAME = "AlohaMaster"
  val ENDPOINT_NAME = "Master"

  def main(argsStr: Array[String]) {
    val conf = new AlohaConf()
    conf.set(RECOVERY_MODE.key, "FILESYSTEM")
    val (rpcEnv, _) = startRpcEnvAndEndpoint("127.0.0.1", 1234 , conf)
    rpcEnv.awaitTermination()
  }

  /**
    * Start the Master and return a three tuple of:
    *   (1) The Master RpcEnv
    *   (2) The REST server bound port, if any
    */
  def startRpcEnvAndEndpoint(
    host: String,
    port: Int,
    conf: AlohaConf): (RpcEnv, Option[Int]) = {
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, conf))

    val applicationDescription2 = ApplicationDescription(
      "test-app",
      "me.jrwang.app.SimpleProcess",
      Array("/Users/jrwang/workspace/sourcecode/aloha/applications/simple"),
      args = "i=0; while [ $i -le 10 ]; do echo $i; ((i++)); sleep 1; done",
      1024,
      1,
      false
    )

    Thread.sleep(10000)
    val resp2 = masterEndpoint.askSync[SubmitApplicationResponse](
      RequestSubmitApplication(applicationDescription2))

    println(resp2)

    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.restPort)
  }
}