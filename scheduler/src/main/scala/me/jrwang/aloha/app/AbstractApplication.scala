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

package me.jrwang.aloha.app

import java.io.File

import scala.concurrent.Promise

import me.jrwang.aloha.common.AlohaConf

abstract class AbstractApplication extends Application {
  protected val result: Promise[ExitState] = Promise()

  protected var appDesc: ApplicationDescription = _
  protected var appDir: File = _
  protected var alohaConf: AlohaConf = _

  override def withDescription(desc: ApplicationDescription): Application = {
    this.appDesc = desc
    this
  }

  override def withApplicationDir(appDir: File): Application = {
    this.appDir = appDir
    this
  }

  override def withAlohaConf(conf: AlohaConf): Application = {
    this.alohaConf = conf
    this
  }

}