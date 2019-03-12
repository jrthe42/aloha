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

package me.jrwang.aloha.example

import com.google.common.base.Charsets
import io.netty.buffer.{ByteBuf, Unpooled}
import me.jrwang.aloha.common.{AlohaConf, Logging}
import me.jrwang.aloha.transport.client.{RpcResponseCallback, TransportClient}
import me.jrwang.aloha.transport.server.RpcHandler
import me.jrwang.aloha.transport.{AlohaTransportConf, TransportContext}

object SimpleAlohaServer extends Logging {
  def main(args: Array[String]): Unit = {
    val transportConf = AlohaTransportConf.fromAlohaConf(new AlohaConf(), "rpc")
    val rpcHandler = new RpcHandler {
      override def receive(client: TransportClient, message: ByteBuf, callback: RpcResponseCallback): Unit = {
        logInfo(s"server receive ${message.toString(Charsets.UTF_8)}")
        callback.onSuccess(Unpooled.wrappedBuffer("hello".getBytes))
      }

      override def channelActive(client: TransportClient): Unit = {
        logInfo("server channel active")
      }

      override def channelInactive(client: TransportClient): Unit = {
        logInfo("server channel inactive")
      }

      override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
        logInfo(s"server exception$cause")
      }
    }

    new TransportContext(transportConf, rpcHandler)
      .createServer("localhost", 9999)
      .awaitTermination()
  }
}

object SimpleAlohaClient extends Logging {
  def main(args: Array[String]): Unit = {
    val transportConf = AlohaTransportConf.fromAlohaConf(new AlohaConf(), "rpc")
    val rpcHandler = new RpcHandler {
      override def receive(client: TransportClient, message: ByteBuf, callback: RpcResponseCallback): Unit = {
        logInfo(s"client receive ${message.toString(Charsets.UTF_8)}")
        callback.onSuccess(Unpooled.wrappedBuffer("hello".getBytes))
      }

      override def channelActive(client: TransportClient): Unit = {
        logInfo("client channel active")
      }

      override def channelInactive(client: TransportClient): Unit = {
        logInfo("client channel inactive")
      }

      override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
        logInfo(s"client exception$cause")
      }
    }

    val client = new TransportContext(transportConf, rpcHandler).createClientFactory()
      .createClient("localhost", 9999)

    client.sendRpc(Unpooled.wrappedBuffer("hello world.".getBytes), new RpcResponseCallback {
      override def onSuccess(response: ByteBuf): Unit = {
        logInfo(s"rpc request success with ${response.toString(Charsets.UTF_8)}")
      }

      override def onFailure(e: Throwable): Unit = {
        logInfo(s"rpc request failed $e")
      }
    })

    client.channel.closeFuture().sync()
  }
}
