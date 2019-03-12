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

package me.jrwang.aloha.transport.client

import java.io.IOException
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

import scala.util.Random
import scala.collection.JavaConverters._

import io.netty.bootstrap.Bootstrap
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption}
import io.netty.channel.socket.SocketChannel
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.common.util.Utils
import me.jrwang.aloha.transport.TransportContext
import me.jrwang.aloha.transport.server.TransportChannelHandler
import me.jrwang.aloha.transport.util.{IOMode, NettyUtils}


/**
  * Factory for creating [[TransportClient]]s by using createClient.
  *
  * The factory maintains a connection pool to other hosts and should return the same
  * TransportClient for the same remote host. It also shares a single worker thread pool for
  * all TransportClients.
  *
  * TransportClients will be reused whenever possible. Prior to completing the creation of a new
  * TransportClient, all given [[TransportClientBootstrap]]s will be run.
  */
class TransportClientFactory(
    val context: TransportContext,
    val clientBootstraps: List[TransportClientBootstrap]
  ) extends AutoCloseable with Logging {

  require(context != null)
  private val conf = context.conf

  /** A simple data structure to track the pool of clients between two peer nodes. */
  final class ClientPool(size: Int) {
    val clients = new Array[TransportClient](size)
    val locks: Array[Object] = Array.fill[Object](size){new Object}
  }

  private val connectionPool = new ConcurrentHashMap[SocketAddress, ClientPool]()
  private val rand = new Random()
  private val numConnectionsPerPeer = conf.numConnectionsPerPeer

  private val ioMode: IOMode = IOMode.valueOf(conf.ioMode)
  private val socketChannelClass = NettyUtils.getClientChannelClass(ioMode)
  private val workerGroup =
    NettyUtils.createEventLoop(ioMode, conf.clientThreads, conf.module + "-client")
  private val pooledAllocator =
    NettyUtils.createPooledByteBufAllocator(conf.preferDirectBufs, false , conf.clientThreads)


  /**
    * Create a [[TransportClient]] connecting to the given remote host / port.
    *
    * We maintains an array of clients and randomly picks one to use.
    * If no client was previously created in the randomly selected spot, this function creates
    * a new client and places it there.
    *
    * This blocks until a connection is successfully established and fully bootstrapped.
    *
    * Concurrency: This method is safe to call from multiple threads.
    */
  @throws[IOException]
  @throws[InterruptedException]
  def createClient(remoteHost: String, remotePort: Int): TransportClient = {
    // Get connection from the connection pool first.
    // If it is not found or not active, create a new one.
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    val unresolvedAddress = InetSocketAddress.createUnresolved(remoteHost, remotePort)
    var clientPool = connectionPool.get(unresolvedAddress)

    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer))
      clientPool = connectionPool.get(unresolvedAddress)
    }

    val clientIndex = rand.nextInt(numConnectionsPerPeer)
    var cachedClient = clientPool.clients(clientIndex)

    if (cachedClient != null && cachedClient.isActive) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      val handler = cachedClient.channel.pipeline
        .get[TransportChannelHandler](classOf[TransportChannelHandler])
      handler.synchronized {
        handler.responseHandler.updateTimeOfLastRequest()
      }

      if (cachedClient.isActive) {
        logTrace(s"Returning cached connection to ${cachedClient.remoteAddress}: $cachedClient")
        return cachedClient
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    val resolvedAddress = new InetSocketAddress(remoteHost, remotePort)
    clientPool.locks(clientIndex) synchronized {
      // Retrieve cache client again, since other thread may update it before.
      cachedClient = clientPool.clients(clientIndex)
      if (null != cachedClient) {
        if (cachedClient.isActive) {
          logTrace(s"Returing cached connection to $resolvedAddress: $cachedClient")
          return cachedClient
        } else {
          logInfo(s"Found inactive connection to $resolvedAddress, creating a new one.")
        }
      }
      clientPool.clients(clientIndex) = createClient(resolvedAddress)
      return clientPool.clients(clientIndex)
    }
  }


  /** Create a completely new [[TransportClient]] to the remote address. */
  @throws[IOException]
  @throws[InterruptedException]
  def createClient(remoteAddr: InetSocketAddress): TransportClient = {
    val bootstrap = new Bootstrap()
      .group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
      .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs)
      .option(ChannelOption.ALLOCATOR, pooledAllocator)

    if (conf.receiveBuf > 0)
      bootstrap.option[java.lang.Integer](ChannelOption.SO_RCVBUF, conf.receiveBuf)
    if (conf.sendBuf > 0)
      bootstrap.option[java.lang.Integer](ChannelOption.SO_SNDBUF, conf.sendBuf)

    val clientRef = new AtomicReference[TransportClient]
    val channelRef = new AtomicReference[Channel]

    bootstrap.handler(new ChannelInitializer[SocketChannel] {
      override def initChannel(ch: SocketChannel): Unit = {
        val clientHandler = context.initializePipeline(ch)
        clientRef.set(clientHandler.client)
        channelRef.set(ch)
      }
    })

    // Connect to the remote server
    val preConnect = System.nanoTime
    val cf = bootstrap.connect(remoteAddr)
    if (!cf.await(conf.connectionTimeoutMs))
      throw new IOException(s"Connecting to $remoteAddr timed out (30000 ms)")
    else if (cf.cause != null)
      throw new IOException(s"Failed to connect to $remoteAddr", cf.cause)

    val client = clientRef.get
    val channel = channelRef.get
    require(client != null, "Channel future completed successfully with null client")

    // Execute any client bootstraps synchronously before marking the Client as successful.
    val preBootstrap = System.nanoTime
    logDebug(s"Connection to $remoteAddr successful, running bootstraps...")

    try {
      clientBootstraps.foreach(_.doBootstrap(client, channel))
    } catch {
      case e: Exception =>
        // catch non-RuntimeExceptions too as bootstrap may be written in Scala
        val bootstrapTimeMs = (System.nanoTime - preBootstrap) / 1000000
        logError(s"Exception while bootstrapping client after $bootstrapTimeMs ms", e)
        client.close()
        throw new RuntimeException(e)
    }
    val postBootstrap = System.nanoTime

    logInfo(s"Successfully created connection to $remoteAddr " +
      s"after ${(postBootstrap - preConnect) / 1000000} ms (" +
      s"${(postBootstrap - preBootstrap) / 1000000} ms spent in bootstraps)")
    client
  }

  override def close(): Unit = {
    // Go through all clients and close them if they are active.
    connectionPool.values().asScala.foreach(p => {
      p.clients.zipWithIndex foreach(f => {
        if(f._1 != null) {
          Utils.closeQuietly(f._1)
          p.clients(f._2) = null
        }
      })
    })
    connectionPool.clear()

    if (workerGroup != null){
      workerGroup.shutdownGracefully()
    }
  }
}