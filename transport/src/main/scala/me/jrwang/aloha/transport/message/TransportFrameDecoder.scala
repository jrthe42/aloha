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

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import scala.collection.mutable

import me.jrwang.aloha.common.Logging


/**
  * A customized frame decoder.
  */
class TransportFrameDecoder extends ChannelInboundHandlerAdapter with Logging {
  private val buffers: mutable.Buffer[ByteBuf] = mutable.Buffer[ByteBuf]()
  private val frameLenBuf: ByteBuf =
    Unpooled.buffer(TransportFrameDecoder.LENGTH_SIZE, TransportFrameDecoder.LENGTH_SIZE)
  private var totalSize: Int = 0
  private var nextFrameSize: Long = TransportFrameDecoder.UNKNOWN_FRAME_SIZE

  override def channelRead(ctx: ChannelHandlerContext, data: Any): Unit = {
    val in = data.asInstanceOf[ByteBuf]
    buffers += in
    totalSize += in.readableBytes

    var break: Boolean = false
    while (buffers.nonEmpty && !break) {
      val frame = decodeNext()
      if (frame != null)
        ctx.fireChannelRead(frame)
      else
        break = true
    }
  }

  private def decodeFrameSize(): Long = {
    if (nextFrameSize != TransportFrameDecoder.UNKNOWN_FRAME_SIZE ||
      totalSize < TransportFrameDecoder.LENGTH_SIZE)
      return nextFrameSize
    // We know there's enough data. If the first buffer contains all the data, great. Otherwise,
    // hold the bytes for the frame length in a composite buffer until we have enough data to read
    // the frame size. Normally, it should be rare to need more than one buffer to read the frame
    // size.
    val first = buffers.head
    if (first.readableBytes >= TransportFrameDecoder.LENGTH_SIZE) {
      nextFrameSize = first.readLong - TransportFrameDecoder.LENGTH_SIZE
      if (!first.isReadable)
        buffers.remove(0).release
    } else {
      while (frameLenBuf.readableBytes < TransportFrameDecoder.LENGTH_SIZE) {
        val next = buffers.head
        val toRead = Math.min(next.readableBytes, TransportFrameDecoder.LENGTH_SIZE - frameLenBuf.readableBytes)
        frameLenBuf.writeBytes(next, toRead)
        if (!next.isReadable)
          buffers.remove(0).release
      }
      nextFrameSize = frameLenBuf.readLong - TransportFrameDecoder.LENGTH_SIZE
      frameLenBuf.clear
    }
    totalSize -= TransportFrameDecoder.LENGTH_SIZE
    nextFrameSize
  }

  private def decodeNext(): ByteBuf = {
    val frameSize = decodeFrameSize()
    if (frameSize == TransportFrameDecoder.UNKNOWN_FRAME_SIZE ||
      totalSize < frameSize) {
      return null
    }
    nextFrameSize = TransportFrameDecoder.UNKNOWN_FRAME_SIZE
    require(frameSize < TransportFrameDecoder.MAX_FRAME_SIZE, s"Too large frame: $frameSize")
    require(frameSize > 0, s"Frame length should be positive: $frameSize")

    var remaining = frameSize.toInt
    if (buffers.head.readableBytes >= remaining) {
      // If the first buffer holds the entire frame, return it.
      nextBufferForFrame(remaining)
    } else {
      // Otherwise, create a composite buffer.
      val frame = buffers.head.alloc.compositeBuffer(Integer.MAX_VALUE)
      while (remaining > 0) {
        val next = nextBufferForFrame(remaining)
        remaining -= next.readableBytes()
        frame.addComponent(next).writerIndex(frame.writerIndex() + next.readableBytes())
      }
      require(remaining == 0)
      frame
    }
  }

  /**
    * Takes the first buffer in the internal list, and either adjust it to fit in the frame
    * (by taking a slice out of it) or remove it from the internal list.
    */
  private def nextBufferForFrame(bytesToRead: Int) = {
    val buf = buffers.head
    val frame = {
      if (buf.readableBytes > bytesToRead) {
        totalSize -= bytesToRead
        buf.retain.readSlice(bytesToRead)
      } else {
        totalSize -= buf.readableBytes
        buffers.remove(0)
        buf
      }
    }
    frame
  }

  @throws[Exception]
  override def channelInactive(ctx: ChannelHandlerContext): Unit = {
    buffers.foreach(_.release())
    frameLenBuf.release
    super.channelInactive(ctx)
  }
}

object TransportFrameDecoder {
  val HANDLER_NAME = "frameDecoder"
  private val LENGTH_SIZE = 8 //long
  private val MAX_FRAME_SIZE = Integer.MAX_VALUE
  private val UNKNOWN_FRAME_SIZE = -1
}