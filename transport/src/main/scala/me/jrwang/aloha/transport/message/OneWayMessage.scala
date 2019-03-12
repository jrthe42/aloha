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

class OneWayMessage(private val message: ByteBuf) extends RequestMessage {
  /** Used to identify this request type. */
  override def `type`: MessageType = MessageType.OneWayMessage

  /** An optional body for the message. */
  override def body: ByteBuf = message

  /** Whether to include the body of the message in the same frame as the message. */
  override def isBodyInFrame: Boolean = true

  /** Number of bytes of the encoded form of this object. */
  override def encodeLength: Int = {
    4
  }

  /**
    * Serializes this object by writing into the given ByteBuf.
    * This method must write exactly encodedLength() bytes.
    */
  override def encode(buf: ByteBuf): Unit = {
    // See comment in encodedLength().
    buf.writeInt(body.readableBytes())
  }

  override def hashCode: Int = Objects.hashCode(body)

  override def equals(other: Any): Boolean = {
    other match {
      case o: OneWayMessage =>
        return super.equals(o)
      case _ =>
    }
    false
  }

  override def toString: String =
    MoreObjects.toStringHelper(this).add("body", body).toString
}

object OneWayMessage {
  def decode(buf: ByteBuf): OneWayMessage = {
    // See comment in encodedLength().
    buf.readInt
    new OneWayMessage(buf.retain)
  }
}