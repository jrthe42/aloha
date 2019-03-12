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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import io.netty.channel.Channel
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.transport.message.{ResponseMessage, RpcFailure, RpcResponse}
import me.jrwang.aloha.transport.server.MessageHandler
import me.jrwang.aloha.transport.util.NettyUtils

/**
  * Handler that processes server responses, in response to requests issued from a
  * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
  * <p>
  * Concurrency: thread safe and can be called from multiple threads.
  */
class TransportResponseHandler(
    channel: Channel
  ) extends MessageHandler[ResponseMessage] with Logging {

  private val outstandingRpcs: JMap[Long, RpcResponseCallback] =
    new ConcurrentHashMap[Long, RpcResponseCallback]()

  /**
    * Records the time (in system nanoseconds) that the last fetch or RPC request was sent.
    */
  private val timeOfLastRequestNs: AtomicLong = new AtomicLong(0)


  /** Handles the receipt of a single message. */
  override def handle(message: ResponseMessage): Unit = {
    message match {
      case resp: RpcResponse =>
        val callback = outstandingRpcs.get(resp.requestId)
        if (callback == null) {
          logWarning(s"Ignoring response for RPC ${resp.requestId} " +
            s"from ${NettyUtils.getRemoteAddress(channel)} (${resp.body.readableBytes()} bytes) " +
            s"since it is not outstanding")
        } else {
          outstandingRpcs.remove(resp.requestId)
          try{
            callback.onSuccess(resp.body)
          } finally {
            //release ByteBuf
            resp.body.release()
          }
        }
      case resp: RpcFailure =>
        val callback = outstandingRpcs.get(resp.requestId)
        if (callback == null) {
          logWarning(s"Ignoring response for RPC ${resp.requestId} " +
            s"from ${NettyUtils.getRemoteAddress(channel)} (${resp.errorString}) " +
            s"since it is not outstanding")
        }
        else {
          outstandingRpcs.remove(resp.requestId)
          callback.onFailure(new RuntimeException(resp.errorString))
        }
      case _ =>
        throw new IllegalStateException("Unknown response type: " + message.`type`)
    }
  }

  /** Invoked when the channel this MessageHandler is on is active. */
  override def channelActive(): Unit = {
    val remoteAddress = NettyUtils.getRemoteAddress(channel)
    logDebug(s"Channel connected to $remoteAddress active ")
  }

  /** Invoked when the channel this MessageHandler is on is inactive. */
  override def channelInactive(): Unit = {
    if (numOutstandingRequests > 0) {
      val remoteAddress = NettyUtils.getRemoteAddress(channel)
      logError(s"Still have $numOutstandingRequests requests outstanding " +
        s" when connection from $remoteAddress is closed")
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"))
    }
  }

  /** Invoked when an exception was caught on the Channel. */
  override def exceptionCaught(cause: Throwable): Unit = {
    if (numOutstandingRequests > 0) {
      val remoteAddress = NettyUtils.getRemoteAddress(channel)
      logError(s"Still have $numOutstandingRequests requests outstanding " +
        s"when connection from $remoteAddress is closed", cause)
      failOutstandingRequests(cause)
    }
  }

  def addRpcRequest(requestId: Long, callback: RpcResponseCallback): Unit = {
    updateTimeOfLastRequest()
    outstandingRpcs.put(requestId, callback)
  }

  def removeRpcRequest(requestId: Long): Unit = {
    outstandingRpcs.remove(requestId)
  }

  /**
    * Fire the failure callback for all outstanding requests. This is called when we have an
    * uncaught exception or pre-mature connection termination.
    */
  private def failOutstandingRequests(cause: Throwable): Unit = {
    outstandingRpcs.asScala foreach(e => {
      try {
        e._2.onFailure(cause)
      } catch {
        case e: Throwable =>
          logWarning("RpcResponseCallback.onFailure throws exception", e)
      }
    })

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingRpcs.clear()
  }

  /**
    * Returns total number of outstanding requests (rpcs)
    */
  private def numOutstandingRequests: Int = {
    outstandingRpcs.size
  }

  /**
    * Returns the time in nanoseconds of when the last request was sent out.
    */
  def getTimeOfLastRequestNs: Long = {
    timeOfLastRequestNs.get
  }

  /**
    * Updates the time of the last request to the current system time.
    */
  def updateTimeOfLastRequest(): Unit = {
    timeOfLastRequestNs.set(System.nanoTime)
  }
}