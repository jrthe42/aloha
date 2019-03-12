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

import com.google.common.base.Throwables
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.util.concurrent.{Future, GenericFutureListener}
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.transport.client.{RpcResponseCallback, TransportClient}
import me.jrwang.aloha.transport.message._


class TransportRequestHandler(
    channel: Channel,
    reverseClient: TransportClient,
    rpcHandler: RpcHandler
  ) extends MessageHandler[RequestMessage] with Logging {
  /** Handles the receipt of a single message. */
  override def handle(message: RequestMessage): Unit = {
    message match {
      case request: RpcRequest =>
        handleRpcRequest(request)
      case msg: OneWayMessage =>
        handleOneWayMessage(msg)
      case _ =>
        throw new IllegalArgumentException(s"Unknown request type: $message")
    }
  }

  private def handleRpcRequest(request: RpcRequest): Unit = {
    try {
      rpcHandler.receive(reverseClient, request.body, new RpcResponseCallback {
        /** Successful serialized result from server. */
        override def onSuccess(response: ByteBuf): Unit = {
          respond(new RpcResponse(request.requestId, response))
        }

        /** Exception either propagated from server or raised on client side. */
        override def onFailure(e: Throwable): Unit = {
          respond(new RpcFailure(request.requestId, Throwables.getStackTraceAsString(e)))
        }
      })
    } catch {
      case e: Throwable =>
        logError(s"Error while invoking RpcHandler#receive() on RPC id ${request.requestId}", e)
        respond(new RpcFailure(request.requestId, Throwables.getStackTraceAsString(e)))
    } finally {
      //release netty ByteBuf, this is last handler
      request.body.release()
    }
  }


  private def handleOneWayMessage(msg: OneWayMessage): Unit = {
    try
      rpcHandler.receive(reverseClient, msg.body)
    catch {
      case e: Throwable =>
        logError("Error while invoking RpcHandler#receive() for one-way message.", e)
    } finally {
      //release netty ByteBuf, this is last handler
      msg.body.release()
    }
  }


  /**
    * Responds to a single message with some Encodable object. If a failure occurs while sending,
    * it will be logged and the channel closed.
    */
  private def respond(result: Encodable) = {
    val remoteAddress = channel.remoteAddress

    channel.writeAndFlush(result).addListener(new GenericFutureListener[Future[_ >: Void]](){
      override def operationComplete(future: Future[_ >: Void]): Unit = {
        if (future.isSuccess) {
          logTrace(s"Sent result $result to client $remoteAddress")
        } else {
          logError(s"Error sending result $result to $remoteAddress; closing connection", future.cause())
          channel.close()
        }
      }
    })
  }

  /** Invoked when an exception was caught on the Channel. */
  override def exceptionCaught(cause: Throwable): Unit = {
    rpcHandler.exceptionCaught(cause, reverseClient)
  }

  /** Invoked when the channel this MessageHandler is on is active. */
  override def channelActive(): Unit = {
    rpcHandler.channelActive(reverseClient)
  }

  /** Invoked when the channel this MessageHandler is on is inactive. */
  override def channelInactive(): Unit = {
    rpcHandler.channelInactive(reverseClient)
  }
}