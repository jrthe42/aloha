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

import java.io.{Closeable, IOException}
import java.net.SocketAddress
import java.util.UUID
import java.util.concurrent.{ExecutionException, TimeUnit}

import com.google.common.base.MoreObjects
import com.google.common.util.concurrent.SettableFuture
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.util.concurrent.{Future, GenericFutureListener}
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.transport.message.{OneWayMessage, RpcRequest}
import me.jrwang.aloha.transport.util.NettyUtils


class TransportClient(
    val channel: Channel,
    val responseHandler: TransportResponseHandler
  ) extends Closeable with Logging{
  @volatile
  private var _timeout = false

  def isActive: Boolean = !_timeout && (channel.isOpen || channel.isActive)

  /**
    * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
    * with the server's response or upon any failure.
    * @param message  The message to send.
    * @param callback Callback to handle the RPC's reply.
    * @return The RPC's id.
    */
  def sendRpc(message: ByteBuf, callback: RpcResponseCallback): Long = {
    val startTime = System.currentTimeMillis
    logTrace(s"Sending RPC to ${NettyUtils.getRemoteAddress(channel)}")
    val requestId = Math.abs(UUID.randomUUID.getLeastSignificantBits)
    responseHandler.addRpcRequest(requestId, callback)
    channel.writeAndFlush(new RpcRequest(requestId, message))
      .addListener(new GenericFutureListener[Future[_ >: Void]] {
        override def operationComplete(future: Future[_ >: Void]): Unit = {
          if (future.isSuccess) {
            val timeTaken: Long = System.currentTimeMillis - startTime
            logTrace(s"Sending request $requestId to ${NettyUtils.getRemoteAddress(channel)} took $timeTaken ms")
          } else {
            val errorMsg: String = s"Failed to send RPC $requestId to ${NettyUtils.getRemoteAddress(channel)}: ${future.cause}"
            logError(s"Failed to send RPC $requestId to ${NettyUtils.getRemoteAddress(channel)}: ${future.cause()}", future.cause())
            responseHandler.removeRpcRequest(requestId)
            channel.close
            try {
              callback.onFailure(new IOException(errorMsg, future.cause))
            } catch {
              case e: Exception =>
                logError("Uncaught exception in RPC response callback handler!", e)
            }
          }
        }
      })
    requestId
  }

  /**
    * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
    * a specified timeout for a response.
    */
  def sendRpcSync(message: ByteBuf, timeoutMs: Long): ByteBuf = {
    val result: SettableFuture[ByteBuf] = SettableFuture.create[ByteBuf]()
    sendRpc(message, new RpcResponseCallback() {
      override def onSuccess(response: ByteBuf): Unit = {
        result.set(response)
      }

      override def onFailure(e: Throwable): Unit = {
        result.setException(e)
      }
    })
    try {
      result.get(timeoutMs, TimeUnit.MILLISECONDS)
    } catch {
      case e: ExecutionException =>
        throw new RuntimeException(e.getCause)
      case e: Exception =>
        throw new RuntimeException(e)
    }
  }

  /**
    * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
    * message, and no delivery guarantees are made.
    *
    * @param message The message to send.
    */
  def send(message: ByteBuf): Unit = {
    channel.writeAndFlush(new OneWayMessage(message))
  }

  /**
    * Removes any state associated with the given RPC.
    *
    * @param requestId The RPC id returned by { @link #sendRpc(ByteBuffer, RpcResponseCallback)}.
    */
  def removeRpcRequest(requestId: Long): Unit = {
    responseHandler.removeRpcRequest(requestId)
  }

  /**
    * Mark this channel as having timed out.
    */
  def timeout(): Unit = {
    this._timeout = true
  }

  def remoteAddress: SocketAddress =
    channel.remoteAddress

  override def close(): Unit = {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close.awaitUninterruptibly(10, TimeUnit.SECONDS)
  }

  override def toString: String =
    MoreObjects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress)
      .add("isActive", isActive)
      .toString
}