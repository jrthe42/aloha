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

package me.jrwang.aloha.common.util

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

object TimeUtils {
  private val timeSuffixes =
    Map(
      "us" -> TimeUnit.MICROSECONDS,
      "ms" -> TimeUnit.MILLISECONDS,
      "s" -> TimeUnit.SECONDS,
      "m" -> TimeUnit.MINUTES,
      "min" -> TimeUnit.MINUTES,
      "h" -> TimeUnit.HOURS,
      "d" -> TimeUnit.DAYS
    )

  /**
    * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count for
    * internal use. If no suffix is provided a direct conversion is attempted.
    */
  def timeStringAs(str: String, unit: TimeUnit) = {
    val lower = str.toLowerCase.trim
    try {
      val m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower)
      if (!m.matches) throw new NumberFormatException("Failed to parse time string: " + str)
      val `val` = m.group(1).toLong
      val suffix = m.group(2)
      // Check for invalid suffixes
      if (suffix != null && !timeSuffixes.contains(suffix))
        throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"")
      // If suffix is valid use that, otherwise none was provided and use the default passed
      unit.convert(`val`, if (suffix != null) timeSuffixes(suffix) else unit)
    } catch {
      case e: NumberFormatException =>
        val timeError = "Time must be specified as seconds (s), " +
          "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
          "E.g. 50s, 100ms, or 250us."
        throw new NumberFormatException(timeError + "\n" + e.getMessage)
    }
  }

  /**
    * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If
    * no suffix is provided, the passed number is assumed to be in ms.
    */
  def timeStringAsMs(str: String): Long =
    timeStringAs(str, TimeUnit.MILLISECONDS)

  /**
    * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
    * no suffix is provided, the passed number is assumed to be in seconds.
    */
  def timeStringAsSeconds(str: String): Long =
    timeStringAs(str, TimeUnit.SECONDS)
}