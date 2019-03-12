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

package me.jrwang.aloha.transport

import me.jrwang.aloha.common.config.ConfigProvider
import me.jrwang.aloha.common.util.TimeUtils


class TransportConf(val module: String, val configProvider: ConfigProvider) {
  private val ALOHA_NETWORK_IO_MODE_KEY = getConfKey("io.mode")
  private val ALOHA_NETWORK_IO_PREFERDIRECTBUFS_KEY = getConfKey("io.preferDirectBufs")
  private val ALOHA_NETWORK_IO_CONNECTIONTIMEOUT_KEY = getConfKey("io.connectionTimeout")
  private val ALOHA_NETWORK_IO_BACKLOG_KEY = getConfKey("io.backLog")
  private val ALOHA_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY = getConfKey("io.numConnectionsPerPeer")
  private val ALOHA_NETWORK_IO_SERVERTHREADS_KEY = getConfKey("io.serverThreads")
  private val ALOHA_NETWORK_IO_CLIENTTHREADS_KEY = getConfKey("io.clientThreads")
  private val ALOHA_NETWORK_IO_RECEIVEBUFFER_KEY = getConfKey("io.receiveBuffer")
  private val ALOHA_NETWORK_IO_SENDBUFFER_KEY = getConfKey("io.sendBuffer")

  def getInt(name: String, defaultValue: Int): Int =
    configProvider.getInt(name, defaultValue)

  def get(name: String, defaultValue: String): String =
    configProvider.getString(name, defaultValue)

  private def getConfKey(suffix: String) =
    "aloha." + module + "." + suffix


  /**
    * IO mode: nio or epoll
    */
  def ioMode: String =
    configProvider.getString(ALOHA_NETWORK_IO_MODE_KEY, "NIO").toUpperCase


  /**
    * If true, we will prefer allocating off-heap byte buffers within Netty.
    */
  def preferDirectBufs: Boolean =
    configProvider.getBoolean(ALOHA_NETWORK_IO_PREFERDIRECTBUFS_KEY, true)


  /**
    * Connect timeout in milliseconds. Default 120 secs.
    */
  def connectionTimeoutMs: Int = {
    val defaultTimeoutMs = TimeUtils.timeStringAsSeconds(
      configProvider.getString(ALOHA_NETWORK_IO_CONNECTIONTIMEOUT_KEY, "120s")) * 1000
    defaultTimeoutMs.toInt
  }

  /**
    * Number of concurrent connections between two nodes for fetching data.
    */
  def numConnectionsPerPeer: Int =
    configProvider.getInt(ALOHA_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY, 1)

  /**
    * Requested maximum length of the queue of incoming connections. Default -1 for no backlog.
    */
  def backLog: Int =
    configProvider.getInt(ALOHA_NETWORK_IO_BACKLOG_KEY, -1)

  /**
    * Number of threads used in the server thread pool. Default to 0, which is 2x#cores.
    */
  def serverThreads: Int = configProvider.getInt(ALOHA_NETWORK_IO_SERVERTHREADS_KEY, 0)

  /**
    * Number of threads used in the client thread pool. Default to 0, which is 2x#cores.
    */
  def clientThreads: Int = configProvider.getInt(ALOHA_NETWORK_IO_CLIENTTHREADS_KEY, 0)

  /**
    * Receive buffer size (SO_RCVBUF).
    * Note: the optimal size for receive buffer and send buffer should be
    * latency * network_bandwidth.
    * Assuming latency = 1ms, network_bandwidth = 10Gbps
    * buffer size should be ~ 1.25MB
    */
  def receiveBuf: Int = configProvider.getInt(ALOHA_NETWORK_IO_RECEIVEBUFFER_KEY, -1)

  /**
    * Send buffer size (SO_SNDBUF).
    */
  def sendBuf: Int = configProvider.getInt(ALOHA_NETWORK_IO_SENDBUFFER_KEY, -1)
}
