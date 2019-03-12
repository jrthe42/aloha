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

package me.jrwang.aloha.rpc.netty

import java.io.{ObjectInputStream, ObjectOutputStream}

import scala.concurrent.Future
import scala.reflect.ClassTag

import me.jrwang.aloha.rpc.{RpcAddress, RpcEndpointAddress, RpcEndpointRef, RpcTimeout}
import me.jrwang.aloha.transport.client.TransportClient


/**
  * The NettyRpcEnv version of RpcEndpointRef.
  *
  * This class behaves differently depending on where it's created. On the node that "owns" the
  * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
  *
  * On other machines that receive a serialized version of the reference, the behavior changes. The
  * instance will keep track of the TransportClient that sent the reference, so that messages
  * to the endpoint are sent over the client connection, instead of needing a new connection to
  * be opened.
  *
  * The RpcAddress of this ref can be null; what that means is that the ref can only be used through
  * a client connection, since the process hosting the endpoint is not listening for incoming
  * connections. These refs should not be shared with 3rd parties, since they will not be able to
  * send messages to the endpoint.
  *
  * @param endpointAddress The address where the endpoint is listening.
  * @param nettyEnv        The RpcEnv associated with this ref.
  */
@SerialVersionUID(1024L)
class NettyRpcEndpointRef(
    private val endpointAddress: RpcEndpointAddress,
    @transient @volatile private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef {

  @transient
  @volatile var client: TransportClient = _

  override def address: RpcAddress =
    if (endpointAddress.rpcAddress != null) endpointAddress.rpcAddress else null

  override def name: String = endpointAddress.name

  override def send(message: Any): Unit = {
    require(message != null,"Message is null")
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    nettyEnv.ask(new RequestMessage(nettyEnv.address, this, message), timeout)
  }

  override def toString:String = s"NettyRpcEndpointRef($endpointAddress)"

  final override def equals(that:Any):Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hashCode():Int =
    if (endpointAddress == null) 0 else endpointAddress.hashCode()

  private def readObject(in: ObjectInputStream):Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream):Unit = {
    out.defaultWriteObject()
  }
}