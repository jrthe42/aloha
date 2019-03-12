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

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel

import io.netty.buffer.ByteBuf
import io.netty.channel.FileRegion
import io.netty.util.{AbstractReferenceCounted, ReferenceCountUtil}

class MessageWithHeader(
    header: ByteBuf,
    body: Any,
    bodyLength: Long
  ) extends AbstractReferenceCounted with FileRegion {
  private val headerLength = header.readableBytes()
  private var totalBytesTransferred = 0L

  /**
    * When the write buffer size is larger than this limit, I/O will be done in chunks of this size.
    * The size should not be too large as it will waste underlying memory copy. e.g. If network
    * available buffer is smaller than this limit, the data cannot be sent within one single write
    * operation while it still will make memory copy with this size.
    */
  private val NIO_BUFFER_LIMIT = 256 * 1024

  require(
    body.isInstanceOf[ByteBuf] || body.isInstanceOf[FileRegion],
    "Body must be a ByteBuf or a FileRegion.")

  override def deallocate(): Unit = {
    header.release
    ReferenceCountUtil.release(body)
  }

  override def retain(): MessageWithHeader = {
    super.retain()
    this
  }

  override def retain(increment: Int): MessageWithHeader = {
    super.retain(increment)
    this
  }

  override def touch(): MessageWithHeader = {
    touch(null)
  }

  override def touch(o: scala.Any): MessageWithHeader = {
    this
  }

  override def position(): Long = 0

  override def transfered(): Long = transferred()

  override def transferred(): Long = totalBytesTransferred

  override def count(): Long = {
    headerLength + bodyLength
  }

  override def transferTo(target: WritableByteChannel, position: Long): Long = {
    require(position == totalBytesTransferred, "Invalid position.")
    // Bytes written for header in this call.
    var writtenHeader = 0
    if (header.readableBytes() > 0) {
      writtenHeader = copyByteBuf(header, target)
      totalBytesTransferred += writtenHeader
      if (header.readableBytes() > 0) return writtenHeader
    }

    // Bytes written for body in this call.
    val writtenBody = body match {
      case region: FileRegion =>
        region.transferTo(target, totalBytesTransferred - headerLength)
      case byteBuf: ByteBuf =>
        copyByteBuf(byteBuf, target)
      case _ =>
        0
    }
    totalBytesTransferred += writtenBody

    writtenHeader + writtenBody
  }

  @throws[IOException]
  private def copyByteBuf(buf: ByteBuf, target: WritableByteChannel): Int = {
    val buffer = buf.nioBuffer()
    val written = {
      if (buffer.remaining() <= NIO_BUFFER_LIMIT) target.write(buffer)
      else writeNioBuffer(target, buffer)
    }
    buf.skipBytes(written)
    written
  }

  @throws[IOException]
  private def writeNioBuffer(writeCh: WritableByteChannel, buf: ByteBuffer): Int = {
    val originalLimit = buf.limit()
    var ret = 0
    try {
      val ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT)
      buf.limit(buf.position() + ioSize)
      ret = writeCh.write(buf)
    } finally {
      buf.limit(originalLimit)
    }
    ret
  }
}