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

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.transport.client.{TransportClient, TransportResponseHandler}
import me.jrwang.aloha.transport.message.{RequestMessage, ResponseMessage}
import me.jrwang.aloha.transport.util.NettyUtils


class TransportChannelHandler (
    val client: TransportClient,
    val responseHandler: TransportResponseHandler,
    val requestHandler: TransportRequestHandler
  ) extends ChannelInboundHandlerAdapter with Logging {

  override def channelRead(ctx: ChannelHandlerContext, request: scala.Any): Unit = {
    request match {
      case requestMessage: RequestMessage =>
        requestHandler.handle(requestMessage)
      case responseMessage: ResponseMessage =>
        responseHandler.handle(responseMessage)
      case _ => ctx.fireChannelRead(request)
    }
  }

  @throws[Exception]
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    try
      requestHandler.channelActive()
    catch {
      case e: Throwable =>
        logError("Exception from request handler while channel is active", e)
        throw e
    }
    try
      responseHandler.channelActive()
    catch {
      case e: Throwable =>
        logError("Exception from response handler while channel is active", e)
        throw e
    }
    super.channelActive(ctx)
  }

  @throws[Exception]
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    try
      requestHandler.channelInactive()
    catch {
      case e: Throwable =>
        logError("Exception from request handler while channel is inactive", e)
        throw e
    }
    try
      responseHandler.channelInactive()
    catch {
      case e: Throwable =>
        logError("Exception from response handler while channel is inactive", e)
        throw e
    }
    super.channelInactive(ctx)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    logWarning(s"Exception in connection from ${NettyUtils.getRemoteAddress(ctx.channel())}", cause)
    requestHandler.exceptionCaught(cause)
    responseHandler.exceptionCaught(cause)
    ctx.close()
  }
}