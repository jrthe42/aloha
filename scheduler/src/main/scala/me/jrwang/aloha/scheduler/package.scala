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

package me.jrwang.aloha

import me.jrwang.aloha.common.config.ConfigBuilder

package object scheduler {
  private[scheduler] val RECOVERY_MODE = ConfigBuilder("aloha.deploy.recoveryMode")
    .stringConf
    .createWithDefault("NONE")

  private[scheduler] val RECOVERY_MODE_FACTORY = ConfigBuilder("aloha.deploy.recoveryMode.factory")
    .stringConf
    .createWithDefault("")

  private[scheduler] val RECOVERY_DIRECTORY = ConfigBuilder("aloha.deploy.recoveryDirectory")
    .stringConf
    .createWithDefault("/tmp/recovery")

  private[scheduler] val ZOOKEEPER_URL = ConfigBuilder("aloha.deploy.zookeeper.url")
    .doc(s"When `${RECOVERY_MODE.key}` is set to ZOOKEEPER, this " +
      "configuration is used to set the zookeeper URL to connect to.")
    .stringConf
    .createOptional

  private[scheduler] val ZOOKEEPER_DIRECTORY = ConfigBuilder("aloha.deploy.zookeeper.dir")
    .stringConf
    .createOptional

  private[scheduler] val MASTER_REST_SERVER_ENABLED = ConfigBuilder("aloha.master.rest.enabled")
    .booleanConf
    .createWithDefault(false)

  private[scheduler] val MASTER_REST_SERVER_PORT = ConfigBuilder("aloha.master.rest.port")
    .intConf
    .createWithDefault(6066)

  private[scheduler] val RETAINED_APPLICATIONS = ConfigBuilder("aloha.deploy.retainedApplications")
    .intConf
    .createWithDefault(200)

  private[scheduler] val REAPER_ITERATIONS = ConfigBuilder("aloha.dead.worker.persistence")
    .intConf
    .createWithDefault(15)

  private[scheduler] val WORKER_TIMEOUT = ConfigBuilder("aloha.worker.timeout")
    .longConf
    .createWithDefault(60)

  private[scheduler] val WORKER_CLEANUP_ENABLED = ConfigBuilder("aloha.worker.cleanup.enabled")
    .booleanConf
    .createWithDefault(false)

  private[scheduler] val WORKER_CLEANUP_INTERVAL = ConfigBuilder("aloha.worker.cleanup.interval")
    .longConf
    .createWithDefault(60 * 30)

  private[scheduler] val APP_DATA_RETENTION = ConfigBuilder("aloha.worker.cleanup.appDataTtl")
    .longConf
    .createWithDefault(7 * 24 * 3600)

  private[scheduler] val WORKER_RETAINED_APPLICATIONS = ConfigBuilder("aloha.worker.retainedApplications")
    .intConf
    .createWithDefault(1000)

  private[scheduler] val EXTRA_LISTENERS = ConfigBuilder("aloha.extraListeners")
    .doc("Class names of listeners to add to Master during initialization.")
    .stringConf
    .toSequence
    .createOptional
}
