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

package me.jrwang.aloha.scheduler.worker

import java.io.{File, IOException}
import java.text.SimpleDateFormat
import java.util.{Date, Locale, UUID}
import java.util.concurrent.{TimeUnit, Future => JFuture, ScheduledFuture => JScheduledFuture}

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import me.jrwang.aloha.app.ApplicationState
import me.jrwang.aloha.common.{AlohaConf, Logging}
import me.jrwang.aloha.common.util.{ThreadUtils, Utils}
import me.jrwang.aloha.scheduler.master.Master
import me.jrwang.aloha.rpc._
import me.jrwang.aloha.scheduler._

class Worker(
    override val rpcEnv: RpcEnv,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    endpointName: String,
    val conf: AlohaConf,
    workDirPath: String = null)
  extends ThreadSafeRpcEndpoint with Logging {

  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port
  Utils.checkHost(host)
  assert(port > 0)

  private val workerId = generateWorkerId()
  private val alohaHome =
    if (sys.props.contains("aloha.testing")) {
      assert(sys.props.contains("aloha.test.home"), "aloha.test.home is not set!")
      new File(sys.props("aloha.test.home"))
    } else {
      new File(sys.env.getOrElse("ALOHA_HOME", "."))
    }
  var workDir: File = _

  // A scheduled executor used to send messages at the specified time.
  private val forwardMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  private val INITIAL_REGISTRATION_RETRIES = 6
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))

  private val CLEANUP_ENABLED = conf.get(WORKER_CLEANUP_ENABLED)
  // How often worker will clean up old app folders
  private val CLEANUP_INTERVAL_MILLIS = conf.get(WORKER_CLEANUP_INTERVAL) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  private val APP_DATA_RETENTION_SECONDS = conf.get(APP_DATA_RETENTION)

  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  private val HEARTBEAT_MS = conf.get(WORKER_TIMEOUT) * 1000 / 4

  private var registerMasterFutures: Array[JFuture[_]] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None

  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.length // Make sure we can register with all masters at the same time
  )

  private var master: Option[RpcEndpointRef] = None
  private var activeMasterUrl: String = ""
  private var registered = false
  private var connected = false

  private var connectionAttemptCount = 0

  val apps = new mutable.HashMap[String, AppRunner] //running apps
  val finishedApps = new mutable.LinkedHashMap[String, AppRunner]
  val retainedApps = conf.get(WORKER_RETAINED_APPLICATIONS)

  val appDirectories = new mutable.HashMap[String, Seq[String]]

  var coresUsed = 0
  var memoryUsed = 0

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.CHINA)
  private def generateWorkerId():String = {
    "worker-%s-%s-%d".format(createDateFormat.format(new Date()), host, port)
  }

  private def createWorkDir() {
    workDir = Option(workDirPath).map(new File(_)).getOrElse(new File(alohaHome, "work"))
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if ( !workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create work directory " + workDir)
        System.exit(1)
      }
      assert (workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create work directory " + workDir, e)
        System.exit(1)
    }
  }

  override def onStart():Unit = {
    assert(!registered)
    logInfo("Starting Aloha worker %s:%d with %d cores, %s RAM".format(host, port,cores,memory))
    createWorkDir()
    registerWithMaster()
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (master.exists(_.address == remoteAddress)) {
      logInfo(s"$remoteAddress Disassociated !")
      masterDisconnected()
    }
  }

  private def masterDisconnected() {
    logError("Connection to master failed! Waiting for master to reconnect...")
    connected = false
    registerWithMaster()
  }

  override def onStop() {
    cancelLastRegistrationRetry()
    registerMasterThreadPool.shutdownNow()
    forwardMessageScheduler.shutdownNow()
  }

  override def receive: PartialFunction[Any, Unit] = synchronized {
    case msg: RegisterWorkerResponse =>
      handleRegisterResponse(msg)

    case ReRegisterWithMaster =>
      reRegisterWithMaster()

    case SendHeartBeat =>
      if (connected) sendToMaster(Heartbeat(workerId, self))

    case MasterChanged(masterRef) =>
      logInfo("Master has changed, new master is at " + masterRef.address.toAlohaURL)
      changeMaster(masterRef)

      val runningApps = apps.values.
        map(e => new AppDesc(e.appId, e.state))
      masterRef.send(WorkerSchedulerStateResponse(workerId, runningApps.toList))

    case ReconnectWorker(masterUrl) =>
      logInfo(s"Master with url $masterUrl requested this worker to reconnect")
      registerWithMaster()

    case WorkDirCleanup =>
      //TODO clean work dir

    case LaunchApplication(masterUrl, appId, appDesc) =>
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo(s"Asked to launch application $appId, name ${appDesc.name}")

          // Create the executor's working directory
          val appDir = new File(workDir, appId)
          if (!appDir.mkdirs()) {
            throw new IOException("Failed to create directory " + appDir)
          }

          val manager = new AppRunner(
            conf,
            appId,
            appDesc,
            self,
            workerId,
            host,
            appDir,
            ApplicationState.RUNNING)

          apps(appId) = manager
          manager.start()
          coresUsed += appDesc.cores
          memoryUsed += appDesc.memory
        } catch {
          case e: Exception =>
            logError(s"Failed to launch application $appId, name ${appDesc.name}.", e)
            if (apps.contains(appId)) {
              apps(appId).kill()
              apps -= appId
            }
            sendToMaster(ApplicationStateChanged(appId, ApplicationState.FAILED,
              Some(e.getMessage), Some(e)))
        }
      }

    case appStateChanged @ ApplicationStateChanged(appId, state, message, exception) =>
      handleApplicationStateChanged(appStateChanged)

    case KillApplication(masterUrl, appId) =>
      if (masterUrl != activeMasterUrl) {
        logWarning(s"Invalid Master ($masterUrl) attempted to kill application $appId" )
      } else {
        apps.get(appId) match {
          case Some(app) =>
            logInfo(s"Asked to kill application $appId")
            app.kill()
          case None =>
            logInfo(s"Asked to kill unknown executor $appId.")
        }
      }
  }

  private def handleRegisterResponse(msg: RegisterWorkerResponse):Unit = synchronized {
    msg match {
      case RegisteredWorker(masterRef) =>
        logInfo("Successfully registered with master " + masterRef.address.toAlohaURL)
        registered = true
        changeMaster(masterRef)
        forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run():Unit = {
            self.send(SendHeartBeat)
          }
        },0, HEARTBEAT_MS, TimeUnit.MILLISECONDS)
        if (CLEANUP_ENABLED) {
          logInfo(s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
          forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = self.send(WorkDirCleanup)
          }, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
        }

        val runningApps = apps.values.
          map(e => new AppDesc(e.appId, e.state))
        masterRef.send(WorkerLatestState(workerId, runningApps.toList))

      case RegisterWorkerFailed(message) =>
        if (!registered) {
          logError("Worker registration failed: " + message)
          System.exit(1)
        }
      case MasterInStandby =>
        // Ignore. Master not yet ready.
    }
  }

  private def registerWithMaster():Unit = {
    registrationRetryTimer match {
      case None =>
        registered = false
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwardMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReRegisterWithMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  /**
    * Re-register with the master because a network failure or a master failure has occurred.
    * If the re-registration attempt threshold is exceeded, the worker exits with error.
    * Note that for thread-safety this should only be called from the rpcEndpoint.
    */
  private def reRegisterWithMaster(): Unit = {
    Utils.tryOrExit({
      connectionAttemptCount += 1
      if (registered) {
        logInfo("Worker already registered to master.")
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        master match {
          case Some(masterRef) =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress = masterRef.address
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run():Unit = {
                try {
                  logInfo(s"Connecting to master $masterAddress ...")
                  val masterEndpoint = rpcEnv.retrieveEndpointRef(masterAddress, Master.ENDPOINT_NAME)
                  sendRegisterMessageToMaster(masterEndpoint)
                } catch {
                  case ie:InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress",e)
                }
              }
            }))
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwardMessageScheduler.scheduleAtFixedRate(
              new Runnable {
                override def run():Unit = Utils.tryLogNonFatalError {
                  self.send(ReRegisterWithMaster)
                }
              },
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    })
  }

  private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to master " + masterAddress + "...")
            val masterEndpoint = rpcEnv.retrieveEndpointRef(masterAddress, Master.ENDPOINT_NAME)
            sendRegisterMessageToMaster(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }

  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.send(RegisterWorker(
      workerId,
      host,
      port,
      self,
      cores,
      memory,
      masterEndpoint.address))
  }

  private def changeMaster(masterRef: RpcEndpointRef):Unit = {
    activeMasterUrl = masterRef.address.toAlohaURL
    master = Some(masterRef)
    connected = true
    cancelLastRegistrationRetry()
  }

  /**
    * Send a message to the current master. If we have not yet registered successfully with any
    * master, the message will be dropped.
    */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }

  /**
    * Cancel last registeration retry, or do nothing if no retry
    */
  private def cancelLastRegistrationRetry():Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  private[worker] def handleApplicationStateChanged(appStateChanged: ApplicationStateChanged):
    Unit = {
    sendToMaster(appStateChanged)
    val appId = appStateChanged.appId
    val state = appStateChanged.state
    val message = appStateChanged.message
    val exception = appStateChanged.exception
    state match {
      case ApplicationState.ERROR =>
        logWarning(s"Application $appId failed with unrecoverable exception: ${exception.get}")
      case ApplicationState.FAILED =>
        logWarning(s"Application $appId exited with failure")
      case ApplicationState.FINISHED =>
        logInfo(s"Application $appId exited successfully")
      case ApplicationState.KILLED =>
        logInfo(s"Application $appId was killed by user")
      case _ =>
        logDebug(s"Application $appId changed state to $state")
    }
    if (ApplicationState.isFinished(state)) {
      apps.get(appId) match {
        case Some(a) =>
          apps -= appId
          finishedApps(appId) = a
          trimFinishedAppsIfNecessary()
          coresUsed -= a.appDesc.cores
          memoryUsed -= a.appDesc.memory
        case None =>
          logInfo("Unknown application " + appId + " with state " + state +
            message.map(" message " + _).getOrElse(""))
      }
    }
  }

  private def trimFinishedAppsIfNecessary(): Unit = {
    // do not need to protect with locks
    // thread-safe RpcEndPoint
    if (finishedApps.size > retainedApps) {
      finishedApps.take(math.max(finishedApps.size / 10, 1)).foreach {
        case (driverId, _) => finishedApps.remove(driverId)
      }
    }
  }
}


object Worker extends Logging {
  val SYSTEM_NAME = "AlohaWorker"
  val ENDPOINT_NAME = "Worker"

  def main(argsStr: Array[String]): Unit = {
    val conf = new AlohaConf()
    val args = new WorkerArguments(argsStr, conf)
    val rpcEnv = startRpcEnvAndEndpoint(args.host, args.port, args.cores,
      args.memory, args.masters, args.workDir, conf = conf)
    rpcEnv.awaitTermination()
  }

  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      cores: Int,
      memory: Int,
      masterUrls: Array[String],
      workDir: String,
      workerNumber: Option[Int] = None,
      conf: AlohaConf = new AlohaConf()): RpcEnv = {

    val systemName = SYSTEM_NAME + workerNumber.map(_.toString).getOrElse("")
    val rpcEnv = RpcEnv.create(systemName, host, port, conf)
    val masterAddresses = masterUrls.map(RpcAddress.fromAlohaURL(_))
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new Worker(rpcEnv, cores, memory,
      masterAddresses, ENDPOINT_NAME, conf, workDir))
    rpcEnv
  }
}