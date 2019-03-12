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

package me.jrwang.aloha.transport

import io.netty.channel.Channel
import io.netty.channel.socket.SocketChannel
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.transport.client.{TransportClient, TransportClientBootstrap, TransportClientFactory, TransportResponseHandler}
import me.jrwang.aloha.transport.message.{MessageDecoder, MessageEncoder, TransportFrameDecoder}
import me.jrwang.aloha.transport.server._

/**
  * Contains the context to create a [[TransportServer]], [[TransportClientFactory]], and to
  * setup Netty Channel pipelines with a [[TransportChannelHandler]].
  */
class TransportContext(
    val conf: TransportConf,
    val rpcHandler: RpcHandler
  ) extends Logging {

  /**
    * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
    * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
    * to create a Client.
    */
  def createClientFactory(bootstraps: List[TransportClientBootstrap]) =
    new TransportClientFactory(this, bootstraps)

  /**
    *  Initializes a ClientFactor
    */
  def createClientFactory(): TransportClientFactory =
    new TransportClientFactory(this, List[TransportClientBootstrap]())

  /**
    * Create a server which will attempt to bind to a specific port.
    */
  def createServer(port: Int, bootstraps: List[TransportServerBootstrap]): TransportServer =
    new TransportServer(this, null, port, rpcHandler, bootstraps)


  /**
    * Create a server which will attempt to bind to a specific host and port.
    */
  def createServer(host: String, port: Int): TransportServer =
    new TransportServer(this, host, port, rpcHandler, List[TransportServerBootstrap]())


  def initializePipeline(channel: SocketChannel): TransportChannelHandler =
    initializePipeline(channel, rpcHandler)

  /**
    * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
    * has a [[TransportChannelHandler]] to handle request or
    * response messages.
    *
    * @param channel           The channel to initialize.
    * @param channelRpcHandler The RPC handler to use for the channel.
    * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
    *         be used to communicate on this channel. The TransportClient is directly associated with a
    *         ChannelHandler to ensure all users of the same channel get the same TransportClient object.
    */
  def initializePipeline(channel: SocketChannel, channelRpcHandler: RpcHandler): TransportChannelHandler = {
    try {
      val channelHandler = createChannelHandler(channel, channelRpcHandler)
      channel.pipeline
        .addLast("encoder", MessageEncoder)
        .addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder())
        .addLast("decoder", MessageDecoder)
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler)
      channelHandler
    } catch {
      case e: RuntimeException =>
        logError("Error while initializing Netty pipeline", e)
        throw e
    }
  }

  /**
    * Creates the server- and client-side handler which is used to handle both RequestMessages and
    * ResponseMessages. The channel is expected to have been successfully created, though certain
    * properties (such as the remoteAddress()) may not be available yet.
    */
  private def createChannelHandler(channel: Channel, rpcHandler: RpcHandler): TransportChannelHandler = {
    val responseHandler = new TransportResponseHandler(channel)
    val client = new TransportClient(channel, responseHandler)
    val requestHandler = new TransportRequestHandler(channel, client, rpcHandler)
    new TransportChannelHandler(client, responseHandler, requestHandler)
  }
}