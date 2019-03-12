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

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import me.jrwang.aloha.scheduler.bus._
import me.jrwang.aloha.app.{ApplicationDescription, ApplicationState}
import me.jrwang.aloha.common.{AlohaConf, AlohaException, Logging}
import me.jrwang.aloha.common.util.{ThreadUtils, Utils}
import me.jrwang.aloha.scheduler.master.zookeeper.ZooKeeperRecoveryModeFactory
import me.jrwang.aloha.rpc._
import me.jrwang.aloha.rpc.serializer.{JavaSerializer, Serializer}
import me.jrwang.aloha.scheduler._
import me.jrwang.aloha.scheduler.rest.StandaloneRestServer

class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    val conf: AlohaConf
  ) extends ThreadSafeRpcEndpoint with LeaderElectable with Logging{
  private val forwardMessageThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")

  private val masterUrl = address.toAlohaURL

  private val workerTimeoutMs = conf.get(WORKER_TIMEOUT) * 1000
  //how long a dead worker keeps in worker list
  private val reaperIterations = conf.get(REAPER_ITERATIONS)

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  private var _listenerBusStarted: Boolean = false
  private var _listenerBus: LiveBus = _

  private val workers = new mutable.HashSet[WorkerInfo]()
  private val idToWorker = new mutable.HashMap[String, WorkerInfo]()
  private val addressToWorker = new mutable.HashMap[RpcAddress, WorkerInfo]()

  val idToApp = new mutable.HashMap[String, ApplicationInfo]
  val apps = new mutable.HashSet[ApplicationInfo]

  private val waitingApps = new ArrayBuffer[ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private val retainedApps = conf.get(RETAINED_APPLICATIONS)

  // For application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA)
  private var nextAppNumber = 0

  private val recoveryMode = conf.get(RECOVERY_MODE)

  private var state = RecoveryState.STANDBY

  private var persistenceEngine: PersistenceEngine = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  // An asynchronous listener bus for Aloha events
  private[aloha] def listenerBus: LiveBus = _listenerBus

  // Alternative application submission gateway that is stable across Aloha versions
  private val restServerEnabled = conf.get(MASTER_REST_SERVER_ENABLED)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

  override def onStart(): Unit = {
    logInfo(s"Starting Aloha master at " + masterUrl)
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(
      new Runnable {
        override def run():Unit = Utils.tryLogNonFatalError(self.send(CheckForWorkerTimeOut))
      }, 0, workerTimeoutMs, TimeUnit.MILLISECONDS)
    try {
      _listenerBus = new LiveBus()
      //Load and start listeners
      setupAndStartListenerBus()
    } catch {
      case e:Throwable =>
        logError("Failed to start Master",e)
        System.exit(-1)
    }

    if (restServerEnabled) {
      val port = conf.get(MASTER_REST_SERVER_PORT)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())

    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = recoveryMode match {
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get(RECOVERY_MODE_FACTORY))
        val factory = clazz.getConstructor(classOf[AlohaConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_
  }

  override def onDisconnected(address: RpcAddress):Unit = {
    //The disconnected client could've been either a worker or an app; remove whichever it was
    logInfo(s"disConnected $address")
    if (addressToWorker.contains(address)) {
      logWarning(s"Worker $address disconnected, remove it.")
      removeWorker(addressToWorker(address), s"Worker $address disconnected")
    }
    if (state == RecoveryState.RECOVERING && canCompleteRecovery) { completeRecovery() }
  }

  override def onConnected(remoteAddress: RpcAddress):Unit = {
    logInfo(s"connected $remoteAddress")
  }

  override def onError(cause:Throwable):Unit = {
    logError(cause.toString)
  }

  override def onStop(): Unit = {
    //prevent the CompleteRecovery message sending to restarted master
    if (recoveryCompletionTask != null) {
      recoveryCompletionTask.cancel(true)
    }
    if (checkForWorkerTimeOutTask != null) {
      checkForWorkerTimeOutTask.cancel(true)
    }
    forwardMessageThread.shutdownNow()
    restServer.foreach(_.stop())
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  /**
    * Registers extra listeners and then starts the listener bus.
    * This should be called after all internal listeners have been registered with the listener bus
    */
  private def setupAndStartListenerBus():Unit = {
    //add event listener to queue
    try {
      conf.get(EXTRA_LISTENERS).foreach { classNames =>
        val listeners = Utils.loadExtensions(classOf[AlohaEventListener], classNames, conf)
        listeners.foreach { listener =>
          listenerBus.addToSharedQueue(listener)
          logInfo(s"Registered listener ${listener.getClass().getName()}")
        }
      }
    } catch {
      case e: Exception =>
        try {
          stop()
        } finally {
          throw new AlohaException(s"Exception when registering AlohaEventListener", e)
        }
    }
    listenerBus.start()
    _listenerBusStarted = true
  }

  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
      val (storedApps, storedWorkers) = persistenceEngine.readPersistedData(rpcEnv)
      state = if (storedApps.isEmpty && storedWorkers.isEmpty) {
        RecoveryState.ALIVE
      } else {
        RecoveryState.RECOVERING
      }
      logInfo("I have been elected as leader! New state: " + state)
      if (state == RecoveryState.RECOVERING) {
        beginRecovery(storedApps, storedWorkers)
        recoveryCompletionTask = forwardMessageThread.schedule(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(CompleteRecovery)
          }
        }, workerTimeoutMs, TimeUnit.MILLISECONDS)
      }

    case CompleteRecovery =>
      completeRecovery()

    case RevokedLeadership =>
      logError("Leadership has been revoked -- master shutting down.")
      System.exit(0)

    case RegisterWorker(id, workerHost, workerPort, workerRef, cores, memory, masterAddr) =>
      logInfo(s"Registering worker $workerHost:$workerPort with $cores cores ${memory}MB memory")
      if (state == RecoveryState.STANDBY) {
        workerRef.send(MasterInStandby)
      } else if (idToWorker.contains(id)) {
        workerRef.send(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory, workerRef)
        if (registerWorker(worker)) {
          persistenceEngine.addWorker(worker)
          workerRef.send(RegisteredWorker(self))
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
          workerRef.send(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }

    case Heartbeat(workerId, worker) =>
      logDebug(s" HeartBeat from worker:$workerId")
      idToWorker.get(workerId) match {
        case Some(workerInfo) =>
          workerInfo.lastHeartBeat = System.currentTimeMillis()
        case None =>
          logWarning(s"Got heartbeat from unregistered worker $workerId")
          worker.send(ReconnectWorker(masterUrl))
      }

    case CheckForWorkerTimeOut => timeOutDeadWorkers()

    case RemoveWorker(worker,reason) =>
      removeWorker(worker, reason)

    case ApplicationStateChanged(appId, state, message, exception) =>
      idToApp.get(appId) match {
        case Some(app) =>
          val oldState = app.state
          if (state == ApplicationState.RUNNING) {
            assert(oldState == ApplicationState.LAUNCHING,
              s"Application $appId state transfer from $oldState to RUNNING is illegal")
          }
          changeApplicationState(state, app, exception, message)

          if(ApplicationState.isFinished(state)) {
            // Remove this application from the worker
            logInfo(s"Removing application $appId because it is $state")
            removeApplication(appId, state, exception, message)
            schedule()
          }

        case None =>
          logWarning(s"Got status update for unknown application $appId")
      }

    case WorkerSchedulerStateResponse(workerId, runningApps) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          logInfo("Worker has been re-registered: " + workerId)
          worker.state = WorkerState.ALIVE
          val validApps = runningApps.filter(app => idToApp.get(app.appId).isDefined)
          validApps.foreach { app =>
            idToApp(app.appId).worker = Some(worker)
            changeApplicationState(ApplicationState.RUNNING, idToApp(app.appId), None,
              Some("Application has been recovered."))
            worker.addApplication(idToApp(app.appId))
          }

        case None =>
          logWarning("Scheduler state from unknown worker: " + workerId)
      }
      if (canCompleteRecovery) { completeRecovery() }

    case WorkerLatestState(workerId, runningApps) =>
      idToWorker.get(workerId) match {
        case Some(worker) =>
          for (appDesc <- runningApps) {
            val appMatches = worker.apps.exists { case (id, _) => id == appDesc.appId }
            if (!appMatches) {
              // master doesn't recognize this app. So just tell worker to kill it.
              worker.endpoint.send(KillApplication(masterUrl, appDesc.appId))
            }
          }
        case None =>
          logWarning("Worker state from unknown worker: " + workerId)
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RequestSubmitApplication(description) =>
      if (state == RecoveryState.STANDBY) {
        val msg = s"Current state is not alive: $state. " +
          "Can only accept application submissions in ALIVE state."
        context.reply(SubmitApplicationResponse(self, false, None, msg))
      } else {
        logInfo("Registering app " + description.name)
        val app = createApplication(description)
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        persistenceEngine.addApplication(app)
        schedule()
        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the state. For now it's simply "fire and forget".
        context.reply(SubmitApplicationResponse(self, true, Some(app.id),
          s"Application successfully submitted as ${app.id}"))
      }

    case RequestKillApplication(appId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"Current state is not alive: $state. " +
          s"Can only kill application in ALIVE state."
        context.reply(KillApplicationResponse(self, success = false, appId, msg))
      } else {
        logInfo("Asked to kill app " + appId)
        idToApp.get(appId) match {
          case Some(a) =>
            if (waitingApps.contains(a)) {
              waitingApps -= a
              self.send(ApplicationStateChanged(appId, ApplicationState.KILLED, Some("Killed")))
            } else {
              // We just notify the worker to kill the app here. The final bookkeeping occurs
              // on the return path when the worker submits a state change back to the master
              // to notify it that the app was successfully killed.
              a.worker.foreach { w =>
                w.endpoint.send(KillApplication(masterUrl, appId))
              }
            }
            // TODO: It would be nice for this to be a synchronous response
            val msg = s"Kill request for $appId submitted"
            logInfo(msg)
            context.reply(KillApplicationResponse(self, success = true, appId, msg))
          case None =>
            val msg = s"App $appId has already finished or does not exist"
            logWarning(msg)
            context.reply(KillApplicationResponse(self, success = false, appId, msg))
        }
      }

    case RequestApplicationStatus(appId) =>
      if (state != RecoveryState.ALIVE) {
        val msg = s"Current state is not alive:: $state. " +
          "Can only request application status in ALIVE state."
        context.reply(
          ApplicationStatusResponse(found = false, None, None, None, Some(new Exception(msg))))
      } else {
        (apps ++ completedApps).find(_.id == appId) match {
          case Some(app) =>
            context.reply(ApplicationStatusResponse(found = true, Some(app.state),
              app.worker.map(_.id), app.worker.map(_.hostPort), app.exception))
          case None =>
            context.reply(ApplicationStatusResponse(found = false, None, None, None, None))
        }
      }

    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, restServerBoundPort,
        workers.toArray, apps.toArray, completedApps.toArray, state))

    case BoundPortsRequest =>
      context.reply(BoundPortsResponse(address.port, restServerBoundPort))
  }

  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
        removeWorker(oldWorker, "Worker replaced by a new worker with same address")
      } else {
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }

    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }

  /**
    * Find dead workers and remove them
    */
  private def timeOutDeadWorkers():Unit = {
    val currentTime = System.currentTimeMillis()
    val toRemove = workers.filter(currentTime - _.lastHeartBeat > workerTimeoutMs)
    for (worker <- toRemove) {
      if (worker.state != WorkerState.DEAD) {
        logWarning(s"Removing ${worker.id} with heartbeat timeout in " +
          s"${(currentTime - worker.lastHeartBeat) / 1000} seconds")
        self.send(RemoveWorker(worker, "worker dead"))
      } else {
        if (worker.lastHeartBeat < currentTime - ((reaperIterations + 1) * workerTimeoutMs)) {
          workers -= worker // we've seen this DEAD worker for long enough; cull it
        }
      }
    }
  }

  private def removeWorker(worker: WorkerInfo, msg: String) {
    logInfo(s"Removing worker ${worker.id} on ${worker.host}:${worker.port}, reason $msg")
    worker.setState(WorkerState.DEAD)
    idToWorker -= worker.id
    addressToWorker -= worker.endpoint.address

    for (app <- worker.apps.values) {
      if (app.desc.supervise) {
        logInfo(s"Re-launching ${app.id}")
        relaunchApplication(app, msg)
      } else {
        logInfo(s"Not re-launching ${app.id} because it was not supervised")
        removeApplication(app.id, ApplicationState.ERROR, None,
          Some(s"Worker for application ${app.id} has been removed."))
      }
    }
    persistenceEngine.removeWorker(worker)
  }

  private def relaunchApplication(app: ApplicationInfo, msg: String) {
    removeApplication(app.id, ApplicationState.RELAUNCHING, None, Some("relaunching it"))
    val newApp = createApplication(app.desc)
    persistenceEngine.addApplication(newApp)
    apps.add(newApp)
    waitingApps += newApp

    val event = AppRelaunchedEvent(app.id, newApp.id, Some(msg))
    postEventToBus(event)

    schedule()
  }

  /** Generate a new app ID given an app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }

  private def createApplication(desc: ApplicationDescription):
    ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    val appId = newApplicationId(date)
    new ApplicationInfo(appId, desc, date, now)
  }

  private def registerApplication(app: ApplicationInfo): Unit = {
    apps += app
    idToApp(app.id) = app
    waitingApps += app
  }

  private def removeApplication(
      appId: String,
      finalState: ApplicationState.Value,
      exception: Option[Exception] = None,
      msg: Option[String] = None) {
    idToApp.get(appId) match {
      case Some(app) =>
        logInfo(s"Removing application: $appId, reason: $msg")
        apps -= app
        idToApp -= appId
        completedApps += app
        persistenceEngine.removeApplication(app)
        app.exception = exception
        changeApplicationState(finalState, app, exception, msg)
        app.worker.foreach(w => w.removeApplication(app))
        //trim completed apps
        if (completedApps.size >= retainedApps) {
          val toRemove = math.max(retainedApps / 10, 1)
          completedApps.trimStart(toRemove)
        }
        schedule()
      case None =>
        logWarning(s"Asked to remove unknown application: $appId")
    }
  }

  private def canCompleteRecovery =
    workers.count(_.state == WorkerState.UNKNOWN) == 0 &&
      apps.count(_.state == ApplicationState.UNKNOWN) == 0

  private def beginRecovery(storedApps: Seq[ApplicationInfo], storedWorkers: Seq[WorkerInfo]) {
    for (app <- storedApps) {
      if(!app.launched) {
        registerApplication(app)
      } else {
        app.state = ApplicationState.UNKNOWN
        apps += app
        idToApp(app.id) = app
      }
    }

    for (worker <- storedWorkers) {
      logInfo("Trying to recover worker: " + worker.id)
      try {
        registerWorker(worker)
        worker.state = WorkerState.UNKNOWN
        worker.endpoint.send(MasterChanged(self))
      } catch {
        case e: Exception => logInfo("Worker " + worker.id + " had exception on reconnect")
      }
    }
  }

  private def completeRecovery() {
    // Ensure "only-once" recovery semantics using a short synchronization period.
    if (state != RecoveryState.RECOVERING) { return }
    state = RecoveryState.COMPLETING_RECOVERY

    // Kill off any workers and apps that didn't respond to us.
    workers.filter(_.state == WorkerState.UNKNOWN).foreach(
      removeWorker(_, "Not responding for recovery"))

    // remove applications which were not claimed by any workers
    apps.filter(_.state == ApplicationState.UNKNOWN).foreach { a =>
      logWarning(s"Application ${a.id} was not found after master recovery")
      removeApplication(a.id, ApplicationState.ERROR, None,
          Some(s"Application ${a.id} was not found after master recovery"))
    }

    state = RecoveryState.ALIVE
    schedule()
    logInfo("Recovery complete - resuming operations!")
  }

  /**
    * Schedule the currently available resources among waiting apps. This method will be called
    * every time a new app joins or resource availability changes.
    */
  private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    for (app <- waitingApps.toList) { // iterate over a copy of waitingApps
      // Filter out workers that don't have enough resources to launch an executor
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(worker => worker.memoryFree >= app.desc.memory &&
          worker.coresFree >= app.desc.cores)
        .sortBy(_.coresFree).reverse

      if(usableWorkers.isEmpty) {
        log.info(s"No usable worker for application $app")
      } else {
        val worker = usableWorkers(0)
        launchApplication(worker, app)
      }
    }
  }

  private def launchApplication(worker: WorkerInfo, app: ApplicationInfo): Unit = {
    logInfo("Launching application " + app.id + " on worker " + worker.id)
    worker.addApplication(app)
    app.worker = Some(worker)
    app.launched = true
    worker.endpoint.send(LaunchApplication(masterUrl, app.id, app.desc))
    waitingApps -= app
    changeApplicationState(ApplicationState.LAUNCHING, app, exception = None)
    //persist launch state so we don't re-register after recovery
    persistenceEngine.updateApplication(app)
  }

  private def changeApplicationState(
      state: ApplicationState.Value,
      app: ApplicationInfo,
      exception: Option[Exception],
      msg: Option[String] = None): Unit = {
    val oldState = app.state
    if(oldState == state) {
      return
    }
    logInfo(s"App [$app] switched from [$oldState] to [$state]")
    app.state = state

    val event = AppStateChangedEvent(app.id, state, msg)
    postEventToBus(event)
  }

  //post event to event bus
  private def postEventToBus(event: AlohaEvent): Unit = {
    listenerBus.post(event)
  }
}


object Master extends Logging {
  val SYSTEM_NAME = "AlohaMaster"
  val ENDPOINT_NAME = "Master"

  def main(argsStr: Array[String]) {
    val conf = new AlohaConf()
    val args = new MasterArguments(argsStr, conf)
    println("==================================================================")
    println(conf.getAll.map(kv => s"${kv._1} : ${kv._2}").mkString("\n"))
    println("==================================================================")
    val (rpcEnv, restPort) = startRpcEnvAndEndpoint(args.host, args.port , conf)
    println(s"Master start on ${rpcEnv.address}")
    restPort.foreach(p => println(s"Rest server listen on port $p"))
    rpcEnv.awaitTermination()
  }

  /**
    * Start the Master and return a three tuple of:
    *   (1) The Master RpcEnv
    *   (2) The REST server bound port, if any
    */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      conf: AlohaConf): (RpcEnv, Option[Int]) = {
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, conf))
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.restPort)
  }

  def startRpcEnvAndEndpointV2(
    host:String,
    port:Int): (RpcEnv, Option[Int]) = {
    val conf = new AlohaConf

    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf)
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME, new Master(rpcEnv, rpcEnv.address, conf))
    //val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)

//    val applicationDescription = ApplicationDescription(
//      "test-simple-app",
//      "me.jrwang.job.SimpleJob",
//      Array("/Users/jrwang/workspace/sourcecode/aloha/applications/simple"),
//      args = null,
//      1024,
//      1,
//      false
//    )
//
//    Thread.sleep(10000)
//
//    val resp = masterEndpoint.askSync[SubmitApplicationResponse](
//      RequestSubmitApplication(applicationDescription))
//
//    println(resp)

    val applicationDescription2 = ApplicationDescription(
      "test-simple-app",
      "me.jrwang.job.SimpleJob",
      Array("/Users/jrwang/workspace/sourcecode/aloha/applications/simplev2"),
      args = null,
      1024,
      1,
      false
    )

    Thread.sleep(10000)

    val resp2 = masterEndpoint.askSync[SubmitApplicationResponse](
      RequestSubmitApplication(applicationDescription2))

    println(resp2)

    (rpcEnv, None)
  }
}