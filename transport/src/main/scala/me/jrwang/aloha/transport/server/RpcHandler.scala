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

import io.netty.buffer.ByteBuf
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.transport.client.{RpcResponseCallback, TransportClient}


trait RpcHandler {
  /**
    * Receive a single RPC message. Any exception thrown while in this method will be sent back to
    * the client in string form as a standard RPC failure.
    *
    * This method will not be called in parallel for a single TransportClient (i.e., channel).
    *
    * @param client   A channel client which enables the handler to make requests back to the sender
    *                 of this RPC. This will always be the exact same object for a particular channel.
    * @param message  The serialized bytes of the RPC.
    * @param callback Callback which should be invoked exactly once upon success or failure of the
    *                 RPC.
    */
  def receive(client: TransportClient, message: ByteBuf, callback: RpcResponseCallback): Unit

  /**
    * Receives an RPC message that does not expect a reply. The default implementation will
    * call "[[RpcHandler.receive(client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback)]]"
    * and log a warning if any of the callback methods are called.
    *
    * @param client  A channel client which enables the handler to make requests back to the sender
    *                of this RPC. This will always be the exact same object for a particular channel.
    * @param message The serialized bytes of the RPC.
    */
  def receive(client: TransportClient, message: ByteBuf): Unit = {
    receive(client, message, RpcHandler.ONE_WAY_CALLBACK)
  }

  /**
    * Invoked when the channel associated with the given client is active.
    */
  def channelActive(client: TransportClient): Unit = {}

  /**
    * Invoked when the channel associated with the given client is inactive.
    * No further requests will come from this client.
    */
  def channelInactive(client: TransportClient): Unit = {}

  def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {}
}

object RpcHandler {
  private class OneWayRpcCallback extends RpcResponseCallback with Logging {
    override def onSuccess(response: ByteBuf): Unit = {
      logWarning("Response provided for one-way RPC.")
    }

    override def onFailure(e: Throwable): Unit = {
      logWarning("Error response provided for one-way RPC.", e)
    }
  }

  private val ONE_WAY_CALLBACK = new OneWayRpcCallback
}