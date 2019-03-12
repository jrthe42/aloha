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

package me.jrwang.aloha.transport.message

import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandler, ChannelHandlerContext}
import io.netty.handler.codec.MessageToMessageDecoder
import me.jrwang.aloha.common.Logging

@ChannelHandler.Sharable
object MessageDecoder extends MessageToMessageDecoder[ByteBuf] with Logging {

  //Be aware that you need to call {@link ReferenceCounted#retain()} on messages
  //that are just passed through if they are of type {@link ReferenceCounted}.
  //This is needed as the {@link MessageToMessageDecoder} will call
  //{@link ReferenceCounted#release()} on decoded messages.
  override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {
    val msgType = MessageType.decode(msg)
    val decodedMsg = decode(msgType, msg)
    require(msgType == decodedMsg.`type`)
    logTrace(s"Received message ${msgType}: ${decodedMsg}")
    out.add(decodedMsg)
  }

  private def decode(msgType: MessageType, in: ByteBuf) = msgType match {
    case MessageType.RpcRequest =>
      RpcRequest.decode(in)
    case MessageType.RpcResponse =>
      RpcResponse.decode(in)
    case MessageType.RpcFailure =>
      RpcFailure.decode(in)
    case MessageType.OneWayMessage =>
      OneWayMessage.decode(in)
    case _ =>
      throw new IllegalArgumentException("Unexpected message type: " + msgType)
  }
}