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

package me.jrwang.aloha.transport.util

import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.{Channel, EventLoopGroup, ServerChannel}
import io.netty.channel.epoll.{EpollEventLoopGroup, EpollServerSocketChannel, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.{NioServerSocketChannel, NioSocketChannel}
import io.netty.util.concurrent.DefaultThreadFactory
import io.netty.util.internal.PlatformDependent
import me.jrwang.aloha.common.Logging


object NettyUtils extends Logging {
  /**
    * Creates a new ThreadFactory which prefixes each thread with the given name.
    */
  def createThreadFactory(threadPoolPrefix: String) =
    new DefaultThreadFactory(threadPoolPrefix, true)

  /**
    * Creates a Netty EventLoopGroup based on the IOMode.
    */
  def createEventLoop(mode: IOMode, numThreads: Int, threadPrefix: String): EventLoopGroup = {
    val threadFactory = createThreadFactory(threadPrefix)
    mode match {
      case IOMode.NIO =>
        new NioEventLoopGroup(numThreads, threadFactory)
      case IOMode.EPOLL =>
        new EpollEventLoopGroup(numThreads, threadFactory)
      case _ =>
        throw new IllegalArgumentException("Unknown io mode: " + mode)
    }
  }

  /**
    * Returns the correct (client) SocketChannel class based on IOMode.
    */
  def getClientChannelClass(mode: IOMode): Class[_ <: Channel] = mode match {
    case IOMode.NIO =>
      classOf[NioSocketChannel]
    case IOMode.EPOLL =>
      classOf[EpollSocketChannel]
    case _ =>
      throw new IllegalArgumentException("Unknown io mode: " + mode)
  }

  /**
    * Returns the correct ServerSocketChannel class based on IOMode.
    */
  def getServerChannelClass(mode: IOMode): Class[_ <: ServerChannel] = mode match {
    case IOMode.NIO =>
      classOf[NioServerSocketChannel]
    case IOMode.EPOLL =>
      classOf[EpollServerSocketChannel]
    case _ =>
      throw new IllegalArgumentException("Unknown io mode: " + mode)
  }

  /**
    * Returns the remote address on the channel or "&lt;unknown remote&gt;" if none exists.
    */
  def getRemoteAddress(channel: Channel): String = {
    if (channel != null && channel.remoteAddress != null) {
      channel.remoteAddress.toString
    } else {
      "<unknown remote>"
    }
  }

  /**
    * Create a pooled ByteBuf allocator but disables the thread-local cache. Thread-local caches
    * are disabled for TransportClients because the ByteBufs are allocated by the event loop thread,
    * but released by the executor thread rather than the event loop thread. Those thread-local
    * caches actually delay the recycling of buffers, leading to larger memory usage.
    */
  def createPooledByteBufAllocator(allowDirectBufs: Boolean, allowCache: Boolean, numCores: Int): PooledByteBufAllocator = {
    val cores = if (numCores == 0) Runtime.getRuntime.availableProcessors else numCores
    new PooledByteBufAllocator(
      allowDirectBufs && PlatformDependent.directBufferPreferred,
      Math.min(getPrivateStaticField("DEFAULT_NUM_HEAP_ARENA"), cores),
      Math.min(getPrivateStaticField("DEFAULT_NUM_DIRECT_ARENA"), if (allowDirectBufs) cores else 0),
      getPrivateStaticField("DEFAULT_PAGE_SIZE"),
      getPrivateStaticField("DEFAULT_MAX_ORDER"),
      if (allowCache) getPrivateStaticField("DEFAULT_TINY_CACHE_SIZE") else 0,
      if (allowCache) getPrivateStaticField("DEFAULT_SMALL_CACHE_SIZE") else 0,
      if (allowCache) getPrivateStaticField("DEFAULT_NORMAL_CACHE_SIZE") else 0,
      true)
  }

  /**
    * Used to get defaults from Netty's private static fields.
    */
  private def getPrivateStaticField(name: String) = try {
    val f = PooledByteBufAllocator.DEFAULT.getClass.getDeclaredField(name)
    f.setAccessible(true)
    f.getInt(null)
  } catch {
    case e: Exception =>
      throw new RuntimeException(e)
  }
}