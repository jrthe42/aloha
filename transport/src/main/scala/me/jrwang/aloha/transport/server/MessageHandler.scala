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

import me.jrwang.aloha.transport.message.Message

/**
  * Handles either request or response messages coming off of Netty. A MessageHandler instance
  * is associated with a single Netty Channel (though it may have multiple clients on the same
  * Channel.)
  */
trait MessageHandler[T <: Message] {
  /** Handles the receipt of a single message. */
  @throws[Exception]
  def handle(message: T): Unit

  /** Invoked when the channel this MessageHandler is on is active. */
  def channelActive(): Unit

  /** Invoked when an exception was caught on the Channel. */
  def exceptionCaught(cause: Throwable): Unit

  /** Invoked when the channel this MessageHandler is on is inactive. */
  def channelInactive(): Unit
}