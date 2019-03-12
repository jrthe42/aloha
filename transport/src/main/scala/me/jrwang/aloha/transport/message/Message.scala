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

import io.netty.buffer.ByteBuf


trait Encodable {
  //Number of bytes encoding this object.
  def encodeLength: Int

  //Serializing this object by writing to ByteBuf.
  //This method must write exactly encodeLength bytes.
  def encode(buf: ByteBuf): Unit
}

trait Message extends Encodable {
  /** Used to identify this request type. */
  def `type`: MessageType

  /** An optional body for the message. */
  def body: ByteBuf

  /** Whether to include the body of the message in the same frame as the message. */
  def isBodyInFrame: Boolean
}

sealed abstract class MessageType(val id: Byte)
    extends Encodable with Ordered[MessageType] {
  override def encodeLength: Int = 1

  override def encode(buf: ByteBuf): Unit = buf.writeByte(id)

  override def compare(that: MessageType): Int = this.id - that.id
}

object MessageType {
  def decode(buf: ByteBuf): MessageType = {
    buf.readByte() match {
      case 0 => RpcRequest
      case 1 => RpcResponse
      case 2 => RpcFailure
      case 3 => OneWayMessage
      case t => UnknownMessageType(t)
    }
  }

  case object RpcRequest extends MessageType(0)
  case object RpcResponse extends MessageType(1)
  case object RpcFailure extends MessageType(2)
  case object OneWayMessage extends MessageType(3)
  case class UnknownMessageType(override val id: Byte) extends MessageType(id)
}

trait RequestMessage extends Message {
}

trait ResponseMessage extends Message {
}