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

import me.jrwang.aloha.common.{AlohaConf, AlohaException, Logging}
import me.jrwang.aloha.scheduler.AlohaUserCodeClassLoaders


trait Application {

  def start(): Promise[ExitState]

  def shutdown(reason: Option[String]): Unit

  def withDescription(desc: ApplicationDescription): Application

  def withApplicationDir(appDir: File): Application

  def withAlohaConf(conf: AlohaConf): Application

  def clean(): Unit
}

object Application extends Logging {
  def create(appDesc: ApplicationDescription): Application = {
    //TODO we should download dependencies and resource files
    logInfo(s"Create module for [$appDesc]")
    val fullClassName = appDesc.entryPoint
    try {
      val urls = appDesc.libs.map(new File(_)).filter(_.exists())
        .flatMap(_.listFiles().filter(_.isFile)).map(_.toURI.toURL)
      val classLoader = AlohaUserCodeClassLoaders.childFirst(urls)
      Thread.currentThread().setContextClassLoader(classLoader)
      val klass = classLoader.loadClass(fullClassName)
      require(classOf[Application].isAssignableFrom(klass),
        s"$fullClassName is not a subclass of ${classOf[Application].getName}.")
      klass.getConstructor().newInstance().asInstanceOf[Application].withDescription(appDesc)
    } catch {
      case _: NoSuchMethodException =>
        throw new AlohaException(
          s"$fullClassName did not have a zero-argument constructor." +
            s"Note: if the class is defined inside of another Scala class, then its constructors " +
            s"may accept an implicit parameter that references the enclosing class; in this case, " +
            s"you must define the class as a top-level class in order to prevent this extra" +
            " parameter from breaking Atom's ability to find a valid constructor.")
      case e: Throwable =>
        throw e
    }
  }
}