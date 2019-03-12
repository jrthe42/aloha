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

import me.jrwang.aloha.common.AlohaConf
import me.jrwang.aloha.rpc.{RpcAddress, RpcEnv}
import me.jrwang.aloha.scheduler.worker.Worker

object LocalWorker {
  val SYSTEM_NAME = "AlohaWorker"
  val ENDPOINT_NAME = "Worker"

  def main(argsStr: Array[String]): Unit = {
    val conf = new AlohaConf()
    val rpcEnv = startRpcEnvAndEndpoint("127.0.0.1", 1235, 1,
      1024, Array("aloha://127.0.0.1:1234"), "./worker", conf = conf)
    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: AlohaConf = new AlohaConf()): RpcEnv = {

    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val rpcEnv = RpcEnv.create(systemName, host, port, conf)
    val masterAddresses = masterUrls.map(RpcAddress.fromAlohaURL(_))
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, cores, memory,
      masterAddresses, ENDPOINT_NAME, conf, workDir))
    rpcEnv
  }
}