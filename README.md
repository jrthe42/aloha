# Aloha

Aloha is a distributed task scheduling and management framework, which can be easily extended, so you can use Aloha to manage various types of tasks. A typical scenario is using Aloha as a unified task management portal. For example, various types of applications, such as Spark tasks, Flink tasks, ETL tasks, etc., are usually submitted on the big data platform. It is necessary to manage these tasks in a unified manner.

The basic implementation of Aloha is based on [Spark](https://github.com/apache/spark)'s scheduling module. We have made some modifications to the **Master** and **Worker** components and provided an extension interface so you can easily integrate various types of applications. The **Master** supports high availability configuration and has state recovery mechanism.

By the way, we separated the **RPC** module from Spark and rewrote the underlying transport module using Scala. The concept of the RPC module is similar as Akka, which is a pretty suitable for studying how RPC works inside distributed system.

## 0. Dependency
- Java 1.8+
- Maven 3.x
- Scala 2.11 (Will be downloaded automatically by maven)

## 1. How to run
### Compile
```shell
mvn clean package -DskipTests
```

Copy `assembly/target/aloha-assembly_2.11-1.0-dist.tar.gz` into a directory and unzip it. Update the configuration files in the `conf` directory as needed.

### Start master
```shell
bash sbin/aloha-daemon.sh start master -h 127.0.0.1 -p 1234
```

Check the log file in `ALOHA_LOG_DIR`, make sure master started successfully.


### Start worker
```shell
bash sbin/aloha-daemon.sh start worker -h 127.0.0.1 aloha://127.0.0.1:1234
```

Check the worker's log file, and you can see something like this:
```
Successfully registered with master aloha://127.0.0.1:1234
```

### Submit tasks via REST api
By default, when master started, it will start a REST service listening on port 6066. So you can submit tasks through REST api.

There is an example of Aloha `Application` in `example-app` module. Copy `exapmle-app-1.0.jar` to the `$ALOHA_HOME/application` directory, and replace the contents of libs in the following command with the corresponding path, submit the task:

```shell
curl -X POST \
-d '{"action":"CreateSubmissionRequest","name":"test","entryPoint":"me.jrwang.app.SimpleProcess","libs":["/path/to/aloha/application"],"args":"i=0; while [ $i -le 10 ]; do echo $i; ((i++)); sleep 1; done","cores":1,"memory":256}' \
http://127.0.0.1:6066/v1/submissions/create
```

The `applicationId` is included in the response, which can be further used to query task status or kill the task. 

Query task status:

```shell
curl http://127.0.0.1:6066/v1/submissions/status/app-20190311221731-0000
```

Kill the task:
```shell
curl -X POST http://127.0.0.1:6066/v1/submissions/kill/app-20190311221731-0000
```

After the task is completed, check the corresponding file in the `$ALOHA_WORKER_DIR` directory, you can see the output of the task.



## 2. How to extend Aloha application

### Application
Aloha was originally designed to manage long-running tasks such as Flink tasks and Spark Streaming tasks, so Aloha abstracts the tasks it manages into `Application` interfaces. The life cycle of the `Application` is managed by `start()` and `shutdown()` method. When application is scheduled to a worker, the `start()` method is call first. When user quest to kill the application, `shutdown()` method is called.

```scala
trait Application {
  def start(): Promise[ExitState]

  def shutdown(reason: Option[String]): Unit

  def withDescription(desc: ApplicationDescription): Application

  def withApplicationDir(appDir: File): Application

  def withAlohaConf(conf: AlohaConf): Application

  def clean(): Unit
}
```

Notice that the return value of the `start()` method is a `Promise` object. When application is stopped, notify the worker by calling the `Promise.success()` method.

It is recommended to add new type of Application by extending `AbstractApplication` or `ApplicationWithProcess`. If your application is launched by starting a new process, use `ApplicationWithProcess`. You can find an example in `example-app`.

### Listener
In many cases, we want to monitor the application's status as soon as possible. For example, when application failed, you may want to send a notification to the user. Aloha provides the listener interface, so you can respond to application's state change in a timely manner.


```scala
trait AlohaEventListener {
  def onApplicationStateChange(event: AppStateChangedEvent): Unit

  def onApplicationRelaunched(event: AppRelaunchedEvent): Unit

  def onOtherEvent(event: AlohaEvent): Unit
}
```

Set `aloha.extraListeners=class.name.of.Listener1,class.name.of.Listener1` in the configuration file to load customize event listeners.

## 3. Configuration
Aloha's load default configuration from `aloha-default.conf` file, you can specify configuration file using `--properties-file file` when start Aloha. The parameters can also be set in VM options like:

```
-Daloha.XXX.XXX=XX -Daloha.XXX.XXX=XXX
```

| Configuration |Dafault| Description |
| --- | --- | --- |
|`aloha.extraListeners`| None |Class names of listeners to add to Master during initialization.|
|`aloha.deploy.recoveryMode`| NONE |The recovery mode setting to recover when Aloha failed and relaunches. Set to FILESYSTEM to enable single-node recovery mode, or set to ZOOKEEPER to use zookeeper based recovery.|
|`aloha.deploy.recoveryDirectory`| /tmp/recovery | When  `aloha.deploy.recoveryMode` is set to FILESYSTEAM, this configuration is used to set the directory in which Aloha will store recovery state, accessible from the Master's perspective.|
|`aloha.deploy.zookeeper.url` | None |When `aloha.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper URL to connect to.|
|`aloha.deploy.zookeeper.dir`| /aloha |When `aloha.deploy.recoveryMode` is set to ZOOKEEPER, this configuration is used to set the zookeeper directory to store recovery state.|
|`aloha.master.rest.enabled`| false | Enable/Disable submitting job to cluster via REST API. |
|`aloha.master.rest.port`| 6066 | The port REST server bounded to when REST server started.|


## 4. Acknowledgement
The development of Aloha is inspired by Spark. Aloha with Apache 2.0 Open Source License retains all copyright, trademark, author’s information from Spark.