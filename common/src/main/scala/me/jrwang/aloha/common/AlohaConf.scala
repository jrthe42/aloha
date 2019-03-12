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

package me.jrwang.aloha.common

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import me.jrwang.aloha.common.config._
import me.jrwang.aloha.common.util.{TimeUtils, Utils}

@SerialVersionUID(1024L)
class AlohaConf(loadDefaults: Boolean = true) extends Cloneable with Serializable {
  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new AlohaConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))

      override def getAll: Iterable[(String, String)] = sys.env
    })
    _reader
  }

  private val settings = new ConcurrentHashMap[String,String]()

  if (loadDefaults) {
    loadFromSystemProperties()
  }

  private[aloha] def loadFromSystemProperties(): AlohaConf = {
    // Load any aloha.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("aloha.")) {
      set(key, value)
    }
    this
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): AlohaConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  def set[T](entry: ConfigEntry[T], value: T): AlohaConf = {
    set(entry.key, entry.stringConverter(value))
    this
  }

  def set[T](entry: OptionalConfigEntry[T], value: T): AlohaConf = {
    set(entry.key, entry.rawStringConverter(value))
    this
  }

  def setIfMissing(key: String, value: String): AlohaConf = {
    if (!settings.contains(key)) set(key,value)
    this
  }

  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /**
    * Return whether the given config is an akka config (e.g. akka.actor.provider).
    */
  def isAkkaConf(name:String):Boolean = name.startsWith("akka.")

  def getAkkaConf: Seq[(String,String)] = {
    getAll.filter { case (k,_) => isAkkaConf(k) }
  }

  /**
    * By using this instead of System.getenv(), environment variables can be mocked
    * in unit tests.
    */
  def getenv(name: String): String = System.getenv(name)

  /** Get a parameter,falling back to a default if not set */
  def get(key: String, defaultValue: String):String = {
    getOption(key).getOrElse {
      set(key, defaultValue)
      defaultValue
    }
  }

  /** Get an optional value, applying variable substitution. */
  def getWithSubstitution(key: String): Option[String] = {
    getOption(key).map(reader.substitute)
  }

  /**
    * Retrieves the value of a pre-defined configuration entry.
    *
    * - The return type if defined by the configuration entry.
    * - This will throw an exception is the config is not optional and the value is not set.
    */
  def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse {
      set(key, defaultValue.toString)
      defaultValue
    }
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse {
      set(key,defaultValue.toString)
      defaultValue
    }
  }

  /** get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /**
    * Get a time parameter as milliseconds, falling back to a default if not set. If no
    * suffix is provided then milliseconds are assumed.
    */
  def getTimeAsMs(key: String, defaultValue: String):Long = {
    TimeUtils.timeStringAsMs(get(key, defaultValue))
  }

  def getTimeAsSeconds(key: String, defaultValue: String):Long = {
    TimeUtils.timeStringAsSeconds(get(key))
  }

  /**
    * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
    * suffix is provided then seconds are assumed.
    *
    * @throws java.util.NoSuchElementException If the time parameter is not set
    */
  def getTimeAsSeconds(key: String):Long = {
    TimeUtils.timeStringAsSeconds(get(key))
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = {
    settings.containsKey(key)
  }

  /** Copy this object */
  override def clone: AlohaConf = {
    val cloned = new AlohaConf(false)
    settings.entrySet().asScala.foreach { e =>
      cloned.set(e.getKey, e.getValue)
    }
    cloned
  }
}