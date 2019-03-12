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

import java.util.regex.Pattern

sealed abstract class ByteUnit(val multiplier: Long) {

  // Interpret the provided number (d) with suffix (u) as this unit type.
  // E.g. KiB.interpret(1, MiB) interprets 1MiB as its KiB representation = 1024k
  def convertFrom(d: Long, u: ByteUnit): Long = {
    u.convertTo(d, this)
  }

  // Convert the provided number (d) interpreted as this unit type to unit type (u).
  def convertTo(d: Long, u: ByteUnit): Long = {
    if (multiplier > u.multiplier) {
      val ratio = multiplier / u.multiplier
      if (Long.MaxValue / ratio < d) {
        throw new IllegalArgumentException(s"Conversion of $d exceeds Long.MAX_VALUE in ${name()}. Try a larger unit (e.g. MiB instead of KiB)")
      }
      d * ratio
    } else {
      // Perform operations in this order to avoid potential overflow
      // when computing d * multiplier
      d / (u.multiplier / multiplier)
    }
  }

  def name(): String
}

object ByteUnit {
  case object BYTE extends ByteUnit(1L) {
    override def name(): String = "BYTE"
  }

  case object KiB extends ByteUnit(1L << 10) {
    override def name(): String = "KB"
  }

  case object MiB extends ByteUnit(1L << 20) {
    override def name(): String = "MB"
  }

  case object GiB extends ByteUnit(1L << 30) {
    override def name(): String = "GB"
  }

  case object TiB extends ByteUnit(1L << 40) {
    override def name(): String = "TB"
  }

  case object PiB extends ByteUnit(1L << 50) {
    override def name(): String = "PB"
  }
}


object ByteUtils {
  private val byteSuffixes =
    Map(
      "b" -> ByteUnit.BYTE,
      "k" -> ByteUnit.KiB,
      "kb" -> ByteUnit.KiB,
      "m" -> ByteUnit.MiB,
      "mb" -> ByteUnit.MiB,
      "g" -> ByteUnit.GiB,
      "gb" -> ByteUnit.GiB,
      "t" -> ByteUnit.TiB,
      "tb" -> ByteUnit.TiB,
      "p" -> ByteUnit.PiB,
      "pb" -> ByteUnit.PiB
    )


  /**
    * Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to a ByteUnit for
    * internal use. If no suffix is provided a direct conversion of the provided default is
    * attempted.
    */
  def byteStringAs(str: String, unit: ByteUnit) = {
    val lower = str.toLowerCase.trim
    try {
      val m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower)
      val fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower)
      if (m.matches) {
        val `val` = m.group(1).toLong
        val suffix = m.group(2)
        // Check for invalid suffixes
        if (suffix != null && !byteSuffixes.contains(suffix))
          throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"")
        // If suffix is valid use that, otherwise none was provided and use the default passed
        unit.convertFrom(`val`, if (suffix != null) byteSuffixes(suffix) else unit)
      }
      else if (fractionMatcher.matches)
        throw new NumberFormatException("Fractional values are not supported. Input was: " + fractionMatcher.group(1))
      else
        throw new NumberFormatException("Failed to parse byte string: " + str)
    } catch {
      case e: NumberFormatException =>
        val timeError = "Size must be specified as bytes (b), " +
          "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
          "E.g. 50b, 100k, or 250m."
        throw new NumberFormatException(timeError + "\n" + e.getMessage)
    }
  }

  /**
    * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for
    * internal use.
    *
    * If no suffix is provided, the passed number is assumed to be in bytes.
    */
  def byteStringAsBytes(str: String): Long = {
    byteStringAs(str, ByteUnit.BYTE)
  }

  /**
    * Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for
    * internal use.
    *
    * If no suffix is provided, the passed number is assumed to be in kibibytes.
    */
  def byteStringAsKb(str: String): Long = {
    byteStringAs(str, ByteUnit.KiB)
  }

  /**
    * Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for
    * internal use.
    *
    * If no suffix is provided, the passed number is assumed to be in mebibytes.
    */
  def byteStringAsMb(str: String): Long = {
    byteStringAs(str, ByteUnit.MiB)
  }

  /**
    * Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for
    * internal use.
    *
    * If no suffix is provided, the passed number is assumed to be in gibibytes.
    */
  def byteStringAsGb(str: String): Long = {
    byteStringAs(str, ByteUnit.GiB)
  }
}