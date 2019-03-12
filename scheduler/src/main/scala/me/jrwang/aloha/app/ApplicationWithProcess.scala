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
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.concurrent.Promise

import com.google.common.io.Files
import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.common.util.{FileAppender, Utils}

abstract class ApplicationWithProcess extends AbstractApplication with Logging {
  private var process: Process = _
  private var stdoutAppender: FileAppender = _
  private var stderrAppender: FileAppender = _

  // Timeout to wait for when trying to terminate an app.
  private val APP_TERMINATE_TIMEOUT_MS = 10 * 1000

  def getProcessBuilder(): ProcessBuilder

  private var stateMonitorThread: Thread = _

  override def start(): Promise[ExitState] = {
    val processBuilder = getProcessBuilder()
    val command = processBuilder.command()
    val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
    logInfo(s"Launch command: $formattedCommand")
    processBuilder.directory(appDir)

    process = processBuilder.start()
    // Redirect its stdout and stderr to files
    val stdout = new File(appDir, "stdout")
    stdoutAppender = FileAppender(process.getInputStream, stdout, alohaConf)

    val header = "Aloha Application Command: %s\n%s\n\n".format(
      formattedCommand, "=" * 40)
    val stderr = new File(appDir, "stderr")
    Files.write(header, stderr, StandardCharsets.UTF_8)
    stderrAppender = FileAppender(process.getErrorStream, stderr, alohaConf)

    stateMonitorThread = new Thread("app-state-monitor-thread") {
      override def run(): Unit = {
        val exitCode = process.waitFor()
        if(exitCode == 0) {
          result.success(ExitState(ExitCode.SUCCESS, Some("success")))
        } else {
          result.success(ExitState(ExitCode.FAILED, Some("failed")))
        }
      }
    }
    stateMonitorThread.start()
    result
  }

  override def shutdown(reason: Option[String]): Unit = {
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      val exitCode = Utils.terminateProcess(process, APP_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      }
    }
  }
}