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

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import me.jrwang.aloha.scheduler.bus.AsyncEventQueue.POISON_PILL

class AlohaEventBus extends EventBus[AlohaEventListener, AlohaEvent] {
  override protected def doPostEvent(listener: AlohaEventListener, event: AlohaEvent): Unit = {
    event match {
      case e: AppStateChangedEvent =>
        listener.onApplicationStateChange(e)
      case e: AppRelaunchedEvent =>
        listener.onApplicationRelaunched(e)
        //TODO other specific event
      case _ =>
        listener.onOtherEvent(event)
    }
  }
}


/**
  * An asynchronous queue for events. All events posted to this queue will be delivered to the child
  * listeners in a separate thread.
  *
  * Delivery will only begin when the `start()` method is called. The `stop()` method should be
  * called when no more events need to be delivered.
  */
class AsyncAlohaEventBus(val name: String, bus: LiveBus) extends AlohaEventBus {
  private val eventQueue = new LinkedBlockingQueue[AlohaEvent]()

  private val started = new AtomicBoolean(false)
  private val stopped = new AtomicBoolean(false)

  private val logDroppedEvent = new AtomicBoolean(false)


  // Keep the event count separately, so that waitUntilEmpty() can be implemented properly;
  // this allows that method to return only when the events in the queue have been fully
  // processed (instead of just dequeued).
  private val eventCount = new AtomicLong()
  private val dispatchThread = new Thread(s"aloha-listener-group-$name") {
    setDaemon(true)

    override def run():Unit = {
      dispatch()
    }
  }

  private def dispatch():Unit = LiveBus.withinListenerThread.withValue(true) {
    try {
      var next: AlohaEvent = eventQueue.take()
      while (next != POISON_PILL) {
        try {
          super.postToAll(next)
        } finally {}
        eventCount.decrementAndGet()
        next = eventQueue.take()
      }
      eventCount.decrementAndGet()
    } catch {
      case ie:InterruptedException =>
        logInfo(s"Stopping listener queue $name.",ie)
    }
  }


  /**
    * Start an asynchronous thread to dispatch events to the underlying listeners.
    */
  private[aloha] def start():Unit = {
    if (started.compareAndSet(false,true)) {
      dispatchThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  /**
    * Stop the listener bus. It will wait until the queued events have been processed, but new
    * events will be dropped.
    */
  private[aloha] def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      eventCount.incrementAndGet()
      eventQueue.put(POISON_PILL)
    }
    // this thread might be trying to stop itself as part of error handling -- we can't join
    // in that case.
    if (Thread.currentThread() != dispatchThread) {
      dispatchThread.join()
    }
  }

  def post(event: AlohaEvent): Unit = {
    if (stopped.get()) {
      return
    }

    eventCount.incrementAndGet()
    if (eventQueue.offer(event)) {
      return
    }

    eventCount.decrementAndGet()
    if (logDroppedEvent.compareAndSet(false,true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError(s"Dropping event from queue $name. " +
        "This likely means one of the listeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
    logTrace(s"Dropping event $event")
  }

  override def removeListenerOnError(listener: AlohaEventListener): Unit = {
    // the listener failed in an unrecoverably way, we want to remove it from the entire
    // LiveListenerBus (potentially stopping a queue if it is empty)
    bus.removeListener(listener)
  }

}

object AsyncEventQueue {
  case object POISON_PILL extends AlohaEvent
}