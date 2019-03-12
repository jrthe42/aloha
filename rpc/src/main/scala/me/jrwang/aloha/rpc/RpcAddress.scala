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

import me.jrwang.aloha.common.AlohaException

/**
  * Address for an RPC environment, with hostname and port.
  */
case class RpcAddress(host: String, port: Int) {
  def hostPort: String = host + ":" + port
  /** Returns a string in the form of "aloha://host:port". */
  def toAlohaURL: String = "aloha://" + hostPort

  override def toString: String = hostPort
}

object RpcAddress {

  /** Return the [[RpcAddress]] represented by `uri`. */
  def fromURIString(uri: String): RpcAddress = {
    val uriObj = new java.net.URI(uri)
    RpcAddress(uriObj.getHost, uriObj.getPort)
  }

  /** Returns the [[RpcAddress]] encoded in the form of "aloha://host:port" */
  def fromAlohaURL(alohaUrl: String): RpcAddress = {
    val (host, port) = RpcAddress.extractHostPortFromAlohaUrl(alohaUrl)
    RpcAddress(host, port)
  }

  private def extractHostPortFromAlohaUrl(alohaUrl: String): (String, Int) = {
    try {
      val uri = new java.net.URI(alohaUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != "aloha" ||
        host == null ||
        port < 0 ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null ||
        uri.getUserInfo != null) {
        throw new AlohaException("Invalid aloha URL: " + alohaUrl)
      }
      (host, port)
    } catch {
      case e:java.net.URISyntaxException =>
        throw new AlohaException("Invalid aloha URL: " + alohaUrl, e)
    }
  }
}

