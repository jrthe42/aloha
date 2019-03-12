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

import io.netty.channel.{ChannelHandler, ChannelHandlerContext}
import io.netty.handler.codec.MessageToMessageEncoder
import me.jrwang.aloha.common.Logging

@ChannelHandler.Sharable
object MessageEncoder extends MessageToMessageEncoder[Message] with Logging {

  //Be aware that you need to call {@link ReferenceCounted#retain()} on messages
  //that are just passed through if they are of type {@link ReferenceCounted}.
  //This is needed as the {@link MessageToMessageEncoder} will call
  //{@link ReferenceCounted#release()} on encoded messages.

  override def encode(ctx: ChannelHandlerContext, msg: Message, out: util.List[AnyRef]): Unit = {
    val messType = msg.`type`
    //[FrameLength(long)][MessageType][Message][MessageBody(optional)]
    val headerLength = 8 + messType.encodeLength + msg.encodeLength
    val frameLength = headerLength + (if(msg.isBodyInFrame) msg.body.readableBytes() else 0)

    val header = ctx.alloc.heapBuffer(headerLength)
    header.writeLong(frameLength)
    messType.encode(header)
    msg.encode(header)
    assert(header.writableBytes() == 0)

    if (msg.isBodyInFrame) {
      out.add(new MessageWithHeader(header, msg.body, msg.body.readableBytes()))
    } else {
      out.add(header)
    }
  }
}