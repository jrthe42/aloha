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

package me.jrwang.aloha.scheduler.master.zookeeper

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import me.jrwang.aloha.common.{AlohaConf, Logging}
import me.jrwang.aloha.scheduler._
import me.jrwang.aloha.scheduler.master.PersistenceEngine
import me.jrwang.aloha.rpc.serializer.Serializer
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

private[master] class ZooKeeperPersistenceEngine(
    conf: AlohaConf, val serializer: Serializer
  ) extends PersistenceEngine with Logging {

  private val workingDir = conf.get(ZOOKEEPER_DIRECTORY).getOrElse("/aloha") + "/master_status"
  private val zk: CuratorFramework = CuratorUtil.newClient(conf)

  CuratorUtil.mkdir(zk, workingDir)

  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(workingDir + "/" + name, obj)
  }

  override def unpersist(name: String): Unit = {
    zk.delete().forPath(workingDir + "/" + name)
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    zk.getChildren.forPath(workingDir).asScala
      .filter(_.startsWith(prefix)).flatMap(deserializeFromFile[T](_))
  }

  override def close() {
    zk.close()
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serialized = serializer.newInstance().serialize(value)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes)
  }

  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData.forPath(workingDir + "/" + filename)
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(workingDir + "/" + filename)
        None
    }
  }
}