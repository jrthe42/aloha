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

import java.util.concurrent._

import scala.concurrent.duration.Duration
import scala.concurrent.{Awaitable, ExecutionContext, ExecutionContextExecutor, TimeoutException}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}
import me.jrwang.aloha.common.AlohaException

object ThreadUtils {
  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(MoreExecutors.newDirectExecutorService())

  /**
    * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
    * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
    * never block.
    */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext

  /**
    * Preferred alternative to `Await.result()`.
    *
    * This method wraps and re-throws any exceptions thrown by the underlying `Await` call, ensuring
    * that this thread's stack trace appears in logs.
    *
    * In addition, it calls `Awaitable.result` directly to avoid using `ForkJoinPool`'s
    * `BlockingContext`. Codes running in the user's thread may be in a thread of Scala ForkJoinPool.
    * As concurrent executions in ForkJoinPool may see some [[ThreadLocal]] value unexpectedly, this
    * method basically prevents ForkJoinPool from running other tasks in the current waiting thread.
    */
  @throws(classOf[AlohaException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
    try {
      // `awaitPermission` is not actually used anywhere so it's safe to pass in null here.
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.result(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new AlohaException("Exception thrown in awaitResult: ", t)
    }
  }

  /**
    * Create a thread factory that names threads with a prefix and also sets the threads to daemon.
    */
  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build()
  }

  /**
    * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
    * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
    */
  def newDaemonCachedThreadPool(
    prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  /**
    * Wrapper over ScheduledThreadPoolExecutor.
    */
  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    // By default, a cancelled task is not automatically removed from the work queue until its delay
    // elapses. We have to enable it manually.
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

  /**
    * Wrapper over newFixedThreadPool. Thread names are formatted as prefix-ID, where ID is a
    * unique, sequentially assigned integer.
    */
  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }
}