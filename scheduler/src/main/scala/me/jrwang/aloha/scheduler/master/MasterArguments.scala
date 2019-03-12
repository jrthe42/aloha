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

package me.jrwang.aloha.scheduler.master

import scala.annotation.tailrec

import me.jrwang.aloha.common.util.Utils
import me.jrwang.aloha.common.{AlohaConf, Logging}

/**
  * Command-line parser for the master.
  */
private[master] class MasterArguments(args: Array[String], conf: AlohaConf) extends Logging {
  var host = Utils.localHostName()
  var port = 7077
  var propertiesFile: String = null

  // Check for settings in environment variables
  if (System.getenv("ALOHA_MASTER_IP") != null) {
    logWarning("ALOHA_MASTER_IP is deprecated, please use ALOHA_MASTER_HOST")
    host = System.getenv("ALOHA_MASTER_IP")
  }

  if (System.getenv("ALOHA_MASTER_HOST") != null) {
    host = System.getenv("ALOHA_MASTER_HOST")
  }
  if (System.getenv("ALOHA_MASTER_PORT") != null) {
    port = System.getenv("ALOHA_MASTER_PORT").toInt
  }

  parse(args.toList)

  // This mutates the AlohaConf, so all accesses to it must be made after this line
  propertiesFile = Utils.loadDefaultAlohaProperties(conf, propertiesFile)

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: value :: tail =>
      port = value.toInt
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil => // No-op

    case _ =>
      printUsageAndExit(1)
  }

  /**
    * Print usage and exit JVM with the given exit code.
    */
  private def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: Master [options]\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST   Hostname to listen on\n" +
        "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
        "  --properties-file FILE Path to a custom ALOHA properties file.\n" +
        "                         Default is conf/aloha-defaults.conf.")
    System.exit(exitCode)
  }
}