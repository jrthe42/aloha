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

import me.jrwang.aloha.common.AlohaConf
import me.jrwang.aloha.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

object HelloWorldServer {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val rpcEnv: RpcEnv = RpcEnv.create("hello-serve", host, 52345, new AlohaConf())
    val helloEndpoint: RpcEndpoint = new HelloEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("hello-service", helloEndpoint)
    rpcEnv.awaitTermination()
  }
}

class HelloEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("Service started.")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      //println(s"receive $msg")
      context.reply(s"$msg")
    }
    case SayBye(msg) => {
      //println(s"receive $msg")
      context.reply(s"Aloha :), $msg")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }
}

case class SayHi(msg: String)

case class SayBye(msg: String)