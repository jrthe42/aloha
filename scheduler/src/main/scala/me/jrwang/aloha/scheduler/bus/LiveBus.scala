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

package me.jrwang.aloha.scheduler.bus

import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.util.DynamicVariable

/**
  * Asynchronously passes AlohaEvents to registered AlohaEventListener.
  *
  * Until `start()` is called, all posted events are only buffered. Only after this listener bus
  * has started will events be actually propagated to all attached listeners. This listener bus
  * is stopped when `stop()` is called, and it will drop further events after stopping.
  */
class LiveBus {
  private val started:AtomicBoolean = new AtomicBoolean(false)
  private val stopped:AtomicBoolean = new AtomicBoolean(false)

  private val queues = new CopyOnWriteArrayList[AsyncAlohaEventBus]()

  /**
    * Start sending events to attached listeners.
    *
    * This first sends out all buffered events posted before this listener bus has started, then
    * listens for any additional events asynchronously while the listener bus is still running.
    * This should only be called once.
    *
    */
  def start():Unit = synchronized {
    if (!started.compareAndSet(false,true)) {
      throw new IllegalStateException("LiveListenerBus already started.")
    }
    queues.asScala.foreach(_.start())
  }


  /** Add a listener to queue shared by all non-internal listeners. */
  def addToSharedQueue(listener: AlohaEventListener):Unit = {
    addToQueue(listener, LiveBus.SHARED_QUEUE)
  }

  /** Add a listener to the executor management queue. */
  def addToManagementQueue(listener: AlohaEventListener): Unit = {
    addToQueue(listener, LiveBus.EXECUTOR_MANAGEMENT_QUEUE)
  }

  /** Add a listener to the application status queue. */
  def addToStatusQueue(listener: AlohaEventListener): Unit = {
    addToQueue(listener, LiveBus.APP_STATUS_QUEUE)
  }

  /** Add a listener to the event log queue. */
  def addToEventLogQueue(listener: AlohaEventListener): Unit = {
    addToQueue(listener, LiveBus.EVENT_LOG_QUEUE)
  }

  /**
    * Add a listener to a specific queue, creating a new queue if needed. Queues are independent
    * of each other (each one uses a separate thread for delivering events), allowing slower
    * listeners to be somewhat isolated from others.
    */
  private def addToQueue(listener: AlohaEventListener, queue: String): Unit = synchronized {
    if (stopped.get()) {
      throw new IllegalStateException("LiveListenerBus is stopped.")
    }

    queues.asScala.find(_.name == queue) match {
      case Some(q) =>
        q.addListener(listener)
      case None =>
        val newQueue = new AsyncAlohaEventBus(queue, this)
        newQueue.addListener(listener)
        if (started.get()) {
          newQueue.start()
        }
        queues.add(newQueue)
    }
  }

  def removeListener(listener: AlohaEventListener): Unit = synchronized {
    // Remove listener from all queues it was added to, and stop queues that have become empty.
    queues.asScala
      .filter { queue =>
        queue.removeListener(listener)
        queue.listeners.isEmpty()
      }
      .foreach { toRemove =>
        if (started.get() && !stopped.get()) {
          toRemove.stop()
        }
        queues.remove(toRemove)
      }
  }

  /** Post an event to all queues. */
  def post(event: AlohaEvent):Unit = {
    if (!stopped.get()) {
      val it = queues.iterator()
      while (it.hasNext) {
        it.next().post(event)
      }
    }
  }

}

private object LiveBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  val SHARED_QUEUE = "shared"

  val APP_STATUS_QUEUE = "appStatus"

  val EXECUTOR_MANAGEMENT_QUEUE = "executorManagement"

  val EVENT_LOG_QUEUE = "eventLog"
}