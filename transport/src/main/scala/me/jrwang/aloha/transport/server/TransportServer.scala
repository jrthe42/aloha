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

package me.jrwang.aloha.transport.server

import java.io.Closeable
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer, ChannelOption}
import io.netty.channel.socket.SocketChannel
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.common.util.Utils
import me.jrwang.aloha.transport.TransportContext
import me.jrwang.aloha.transport.util.{IOMode, NettyUtils}


class TransportServer(
    transportContext: TransportContext,
    hostToBind: String,
    portToBind: Int,
    appRpcHandler: RpcHandler,
    bootstraps: List[TransportServerBootstrap]
  ) extends Closeable with Logging {
  private val conf  = transportContext.conf

  private var port: Int = -1
  private var bootstrap: ServerBootstrap = _
  private var channelFuture: ChannelFuture = _

  try
    init()
  catch {
    case e: RuntimeException =>
      Utils.closeQuietly(this)
      throw e
  }

  def init(): Unit = {
    val ioMode = IOMode.valueOf(conf.ioMode)
    val bossGroup = NettyUtils.createEventLoop(ioMode, conf.serverThreads, conf.module + "-server")
    val workerGroup = bossGroup
    val allocator = NettyUtils.createPooledByteBufAllocator(conf.preferDirectBufs, true , conf.serverThreads)

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NettyUtils.getServerChannelClass(ioMode))
      .option(ChannelOption.ALLOCATOR, allocator)
      .childOption(ChannelOption.ALLOCATOR, allocator)

    if (conf.backLog > 0)
      bootstrap.option[java.lang.Integer](ChannelOption.SO_BACKLOG, conf.backLog)
    if (conf.receiveBuf > 0)
      bootstrap.childOption[java.lang.Integer](ChannelOption.SO_RCVBUF, conf.receiveBuf)
    if (conf.sendBuf > 0)
      bootstrap.childOption[java.lang.Integer](ChannelOption.SO_SNDBUF, conf.sendBuf)

    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      override protected def initChannel(ch: SocketChannel): Unit = {
        val rpcHandler = bootstraps.foldLeft[RpcHandler](appRpcHandler)((r, b) => {
          b.doBootstrap(ch, r)
        })
        transportContext.initializePipeline(ch, rpcHandler)
      }
    })

    val address = if (hostToBind == null)
      new InetSocketAddress(portToBind)
    else
      new InetSocketAddress(hostToBind, portToBind)
    channelFuture = bootstrap.bind(address)
    channelFuture.syncUninterruptibly
    port = channelFuture.channel.localAddress.asInstanceOf[InetSocketAddress].getPort
    logDebug(s"Transport server started on port: $port")
  }

  def getPort: Int = {
    if (port == -1)
      throw new IllegalStateException("Server not initialized")
    port
  }

  def awaitTermination(): Unit = {
    channelFuture.channel().closeFuture().sync()
  }

  override def close(): Unit = {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel.close.awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.config().group() != null)
      bootstrap.config().group().shutdownGracefully
    if (bootstrap != null && bootstrap.config().childGroup() != null)
      bootstrap.config().childGroup().shutdownGracefully
    bootstrap = null
  }
}