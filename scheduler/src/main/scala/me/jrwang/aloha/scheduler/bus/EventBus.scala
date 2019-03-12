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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import me.jrwang.aloha.common.Logging
import me.jrwang.aloha.common.util.Utils


trait EventBus[L <: AnyRef, E] extends Logging {
  private[bus] val listeners = new CopyOnWriteArrayList[L]()

  /**
    * Add a listener to listen events. This method is thread-safe and can be called in any thread.
    */
  final def addListener(listener: L):Unit = {
    listeners.add(listener)
  }

  /**
    * Remove a listener and it won't receive any events. This method is thread-safe and can be called
    * in any thread.
    */
  final def removeListener(listener: L):Unit = {
    listeners.asScala.find(_ eq listener).foreach(listeners.remove)
  }

  /**
    * This can be overridden by subclasses if there is any extra cleanup to do when removing a
    * listener.  In particular AsyncEventQueues can clean up queues in the LiveListenerBus.
    */
  def removeListenerOnError(listener: L): Unit = {
    removeListener(listener)
  }

  protected def doPostEvent(listener: L, event: E):Unit

  def postToAll(event: E): Unit = {
    val iter = listeners.iterator()
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        doPostEvent(listener,event)
        if (Thread.interrupted()) {
          // We want to throw the InterruptedException right away so we can associate the interrupt
          // with this listener, as opposed to waiting for a queue.take() etc. to detect it.
          throw new InterruptedException()
        }
      } catch {
        case ie: InterruptedException =>
          logError(s"Interrupted while posting to ${Utils.getFormattedClassName(listener)}.  " +
            s"Removing that listener.", ie)
          removeListenerOnError(listener)
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} throws an Exception.", e)
      }
    }
  }
}