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

package me.jrwang.aloha.common.config

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

/**
  * A source of configuration values.
  */
private[aloha] trait ConfigProvider {
  def get(name: String): Option[String]

  def getAll: Iterable[(String, String)]

  def getString(name: String, defaultValue: String): String = {
    get(name) match {
      case Some(str) =>
        str
      case None =>
        defaultValue
    }
  }

  def getInt(name: String, default: Int): Int = {
    get(name) match {
      case Some(v) =>
        v.toInt
      case None =>
        default
    }
  }

  def getLong(name: String, default: Long): Long = {
    get(name) match {
      case Some(v) =>
        v.toLong
      case None =>
        default
    }
  }

  def getDouble(name: String, default: Double): Double = {
    get(name) match {
      case Some(v) =>
        v.toDouble
      case None =>
        default
    }
  }

  def getBoolean(name: String, default: Boolean): Boolean = {
    get(name) match {
      case Some(v) =>
        v.toBoolean
      case None =>
        default
    }
  }
}

private[aloha] class EnvProvider extends ConfigProvider {
  override def get(key: String):Option[String] = sys.env.get(key)

  override def getAll: Iterable[(String, String)] = sys.env
}

private[aloha] class SystemProvider extends ConfigProvider {
  override def get(key: String):Option[String] = sys.props.get(key)

  override def getAll: Iterable[(String, String)] = sys.props
}

private[aloha] class MapProvider(conf: JMap[String, String]) extends ConfigProvider {
  override def get(key:String):Option[String] = Option(conf.get(key))

  override def getAll: Iterable[(String, String)] = conf.asScala
}

/**
  * A config provider that only reads Aloha config keys.
  */
private[aloha] class AlohaConfigProvider(conf: JMap[String, String]) extends ConfigProvider {
  override def get(key:String):Option[String] = {
    if (key.startsWith("aloha.")) {
      Option(conf.get(key))
    } else {
      None
    }
  }

  override def getAll: Iterable[(String, String)] = {
    conf.asScala.filter(_._1.startsWith("aloha."))
  }
}