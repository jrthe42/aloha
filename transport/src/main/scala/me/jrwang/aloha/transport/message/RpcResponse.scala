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

import com.google.common.base.{MoreObjects, Objects}
import io.netty.buffer.ByteBuf


class RpcResponse(val requestId: Long, private val message: ByteBuf) extends ResponseMessage  {
  /** Used to identify this request type. */
  override def `type`: MessageType = MessageType.RpcResponse

  /** An optional body for the message. */
  override def body: ByteBuf = message

  /** Whether to include the body of the message in the same frame as the message. */
  override def isBodyInFrame: Boolean = true

  override def encodeLength: Int = {
    // The integer (a.k.a. the body size) is not really used, since that information is already
    // encoded in the frame length. But this maintains backwards compatibility with versions of
    // RpcRequest that use Encoders.ByteArrays.
    8 + 4
  }

  override def encode(buf: ByteBuf): Unit = {
    buf.writeLong(requestId)
    // See comment in encodedLength().
    buf.writeInt(body.readableBytes())
  }

  override def hashCode: Int = Objects.hashCode(requestId.asInstanceOf[AnyRef], body)

  override def equals(other: Any): Boolean = {
    other match {
      case o: RpcResponse =>
        return requestId == o.requestId && super.equals(o)
      case _ =>
    }
    false
  }

  override def toString: String = MoreObjects.toStringHelper(this)
    .add("requestId", requestId)
    .add("body", body)
    .toString
}

object RpcResponse {
  def decode(buf: ByteBuf): RpcResponse = {
    val requestId = buf.readLong()
    // See comment in encodedLength().
    buf.readInt()
    new RpcResponse(requestId, buf.retain())
  }
}