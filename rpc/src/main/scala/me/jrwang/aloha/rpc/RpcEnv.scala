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

package me.jrwang.aloha.rpc

import scala.concurrent.Future
import scala.concurrent.duration._

import me.jrwang.aloha.common.AlohaConf
import me.jrwang.aloha.rpc.netty.NettyRpcEnvFactory


private[aloha] object RpcEnv {
  def create(
    name: String,
    host: String,
    port: Int,
    conf: AlohaConf,
    clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, 0, clientMode)
  }

  def create(
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    conf: AlohaConf,
    numUsableCores: Int,
    clientMode: Boolean): RpcEnv = {
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port,
      numUsableCores, clientMode)
    new NettyRpcEnvFactory().create(config)
  }
}

/**
  * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
  * so that it can be created via Reflection.
  */
trait RpcEnv {
  private[rpc] val defaultLookupTimeout = RpcTimeout(60.seconds)

  /**
    * Return the address that [[RpcEnv]] is listening to.
    */
  def address: RpcAddress

  /**
    * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
    * guarantee thread-safety.
    */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
    * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
    */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
    */
  def asyncRetrieveEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
    */
  def retrieveEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult[RpcEndpointRef](asyncRetrieveEndpointRefByURI(uri))
  }

  /**
    * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
    * This is a blocking action.
    */
  def retrieveEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    retrieveEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /**
    * Stop [[RpcEndpoint]] specified by `endpointRef`.
    */
  def stop(endpointRef: RpcEndpointRef): Unit

  /**
    * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
    * call [[awaitTermination()]] straight after [[shutdown()]].
    */
  def shutdown(): Unit

  /**
    * Wait until [[RpcEnv]] exits.
    *
    * TODO do we need a timeout parameter?
    */
  def awaitTermination(): Unit

  /**
    * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
    * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
    */
  def deserialize[T](deserializationAction: () => T): T
}

trait RpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv
}