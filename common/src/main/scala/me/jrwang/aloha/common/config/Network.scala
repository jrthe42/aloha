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

package me.jrwang.aloha.common.config

import java.util.concurrent.TimeUnit

private[aloha] object Network {
  private[aloha] val NETWORK_TIMEOUT =
    ConfigBuilder("aloha.network.timeout")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("120s")

  private[aloha] val NETWORK_TIMEOUT_INTERVAL =
    ConfigBuilder("aloha.network.timeoutInterval")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  private[aloha] val RPC_ASK_TIMEOUT =
    ConfigBuilder("aloha.rpc.askTimeout")
      .stringConf
      .createOptional

  private[aloha] val RPC_CONNECT_THREADS =
    ConfigBuilder("aloha.rpc.connect.threads")
      .intConf
      .createWithDefault(64)

  private[aloha] val RPC_IO_NUM_CONNECTIONS_PER_PEER =
    ConfigBuilder("aloha.rpc.io.numConnectionsPerPeer")
      .intConf
      .createWithDefault(1)

  private[aloha] val RPC_IO_THREADS =
    ConfigBuilder("aloha.rpc.io.threads")
      .intConf
      .createOptional

  private[aloha] val RPC_LOOKUP_TIMEOUT =
    ConfigBuilder("aloha.rpc.lookupTimeout")
      .stringConf
      .createOptional

  private[aloha] val RPC_MESSAGE_MAX_SIZE =
    ConfigBuilder("aloha.rpc.message.maxSize")
      .intConf
      .createWithDefault(128)

  private[aloha] val RPC_NETTY_DISPATCHER_NUM_THREADS =
    ConfigBuilder("aloha.rpc.netty.dispatcher.numThreads")
      .intConf
      .createOptional

  private[aloha] val RPC_NUM_RETRIES =
    ConfigBuilder("aloha.rpc.numRetries")
      .intConf
      .createWithDefault(3)

  private[aloha] val RPC_RETRY_WAIT =
    ConfigBuilder("aloha.rpc.retry.wait")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")


  private[aloha] val SERIALIZER_OBJECT_STREAM_RESET =
    ConfigBuilder("aloha.serializer.objectStreamReset")
      .intConf
      .createWithDefault(100)

  private[aloha] val SERIALIZER_EXTRA_DEBUG_INFO =
    ConfigBuilder("aloha.serializer.extraDebugInfo")
      .booleanConf
      .createWithDefault(true)
}