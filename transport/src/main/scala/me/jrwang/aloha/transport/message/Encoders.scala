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

import java.nio.charset.StandardCharsets

import io.netty.buffer.ByteBuf


object Encoders {
  object Strings {
    def encodedLength(s: String): Int = 4 + s.getBytes(StandardCharsets.UTF_8).length

    def encode(buf: ByteBuf, s: String): Unit = {
      val bytes = s.getBytes(StandardCharsets.UTF_8)
      buf.writeInt(bytes.length)
      buf.writeBytes(bytes)
    }

    def decode(buf: ByteBuf): String = {
      val length = buf.readInt
      val bytes = new Array[Byte](length)
      buf.readBytes(bytes)
      new String(bytes, StandardCharsets.UTF_8)
    }
  }
}