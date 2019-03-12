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

package object config {
  private[aloha] val EXECUTOR_LOGS_ROLLING_STRATEGY =
    ConfigBuilder("aloha.executor.logs.rolling.strategy").stringConf.createWithDefault("")

  private[aloha] val EXECUTOR_LOGS_ROLLING_TIME_INTERVAL =
    ConfigBuilder("aloha.executor.logs.rolling.time.interval").stringConf.createWithDefault("daily")

  private[aloha] val EXECUTOR_LOGS_ROLLING_MAX_SIZE =
    ConfigBuilder("aloha.executor.logs.rolling.maxSize")
      .stringConf
      .createWithDefault((1024 * 1024).toString)

  private[aloha] val EXECUTOR_LOGS_ROLLING_MAX_RETAINED_FILES =
    ConfigBuilder("aloha.executor.logs.rolling.maxRetainedFiles").intConf.createWithDefault(-1)

  private[aloha] val EXECUTOR_LOGS_ROLLING_ENABLE_COMPRESSION =
    ConfigBuilder("aloha.executor.logs.rolling.enableCompression")
      .booleanConf
      .createWithDefault(false)
}
