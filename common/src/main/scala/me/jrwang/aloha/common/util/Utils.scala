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

import java.io._
import java.lang.reflect.InvocationTargetException
import java.net.{BindException, Inet4Address, InetAddress, NetworkInterface}
import java.nio.charset.StandardCharsets
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.{ControlThrowable, NonFatal}

import com.google.common.net.InetAddresses
import io.netty.channel.unix.Errors.NativeIoException
import me.jrwang.aloha.common.{AlohaConf, AlohaException, Logging}
import org.apache.commons.lang3.SystemUtils

object Utils extends Logging {
  /**
    * Returns the system properties map that is thread-safe to iterator over. It gets the
    * properties which have been set explicitly, as well as those for which only a default value
    * has been defined.
    */
  def getSystemProperties: Map[String, String] = {
    System.getProperties.stringPropertyNames().asScala
      .map(key => (key, System.getProperty(key))).toMap
  }

  /**
    * Load default Aloha properties from the given file. If no file is provided,
    * use the common defaults file. This mutates state in the given AlohaConf and
    * in this JVM's system properties if the config specified in the file is not
    * already set. Return the path of the properties file used.
    */
  def loadDefaultAlohaProperties(conf: AlohaConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("aloha.")
      }.foreach { case (k, v) =>
        conf.setIfMissing(k, v)
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }

  /**
    * Implements the same logic as JDK `java.lang.String#trim` by removing leading and trailing
    * non-printable characters less or equal to '\u0020' (SPACE) but preserves natural line
    * delimiters according to [[java.util.Properties]] load method. The natural line delimiters are
    * removed by JDK during load. Therefore any remaining ones have been specifically provided and
    * escaped by the user, and must not be ignored
    *
    * @param str
    * @return the trimmed value of str
    */
  private[util] def trimExceptCRLF(str: String): String = {
    val nonSpaceOrNaturalLineDelimiter: Char => Boolean = { ch =>
      ch > ' ' || ch == '\r' || ch == '\n'
    }

    val firstPos = str.indexWhere(nonSpaceOrNaturalLineDelimiter)
    val lastPos = str.lastIndexWhere(nonSpaceOrNaturalLineDelimiter)
    if (firstPos >= 0 && lastPos >= 0) {
      str.substring(firstPos, lastPos + 1)
    } else {
      ""
    }
  }

  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala
        .map { k => (k, trimExceptCRLF(properties.getProperty(k))) }
        .toMap

    } catch {
      case e: IOException =>
        throw new AlohaException(s"Failed when loading Aloha properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  /** Return the path of the default aloha properties file. */
  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("ALOHA_CONF_DIR")
      .orElse(env.get("ALOHA_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}aloha-default.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  /**
    * Terminates a process waiting for at most the specified duration.
    *
    * @return the process exit value if it was successfully terminated, else None
    */
  def terminateProcess(process: Process, timeoutMs: Long): Option[Int] = {
    // Politely destroy first
    process.destroy()
    if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
      // Successful exit
      Option(process.exitValue())
    } else {
      try {
        process.destroyForcibly()
      } catch {
        case NonFatal(e) => logWarning("Exception when attempting to kill process", e)
      }
      // Wait, again, although this really should return almost immediately
      if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
        Option(process.exitValue())
      } else {
        logWarning("Timed out waiting to forcibly kill process")
        None
      }
    }
  }

  def closeQuietly(closeable: Closeable): Unit = {
    try
        if (closeable != null) closeable.close()
    catch {
      case e: IOException =>
        logError("IOException should not have been thrown.", e)
    }
  }

  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case e:ControlThrowable => throw e
      case t:Throwable => t.printStackTrace(); System.exit(-1)
    }
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit) {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}",t)
    }
  }

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  /**
    * Execute a block of code, then a finally block, but if exceptions happen in
    * the finally block, do not suppress the original exception.
    *
    * This is primarily an issue with `finally { out.close() }` blocks, where
    * close needs to be called to clean up `out`, but if an exception happened
    * in `out.write`, it's likely `out` may be corrupted and `out.close` will
    * fail as well. This would then suppress the original/likely more meaningful
    * exception from the original `out.write` call.
    */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }


  /**
    * Execute the given block, logging and re-throwing any uncaught exception.
    * This is particularly useful for wrapping code that runs in a thread, to ensure
    * that exceptions are printed, and to avoid having to catch Throwable.
    */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  /** Executes the given block in a Try, logging any uncaught exceptions. */
  def tryLog[T](f: => T): Try[T] = {
    try {
      val res = f
      scala.util.Success(res)
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        scala.util.Failure(t)
    }
  }

  /** Returns true if the given exception was fatal. See docs for scala.util.control.NonFatal. */
  def isFatalError(e: Throwable): Boolean = {
    e match {
      case NonFatal(_) |
           _: InterruptedException |
           _: NotImplementedError |
           _: ControlThrowable |
           _: LinkageError =>
        false
      case _ =>
        true
    }
  }

  /**
    * Return whether the exception is caused by an address-port collision when binding.
    */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: NativeIoException =>
        (e.getMessage != null && e.getMessage.startsWith("bind() failed: ")) ||
          isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
    * Attempt to start a service on the given port, or fail after a number of attempts.
    * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
    *
    * @param startPort The initial port to start the service on.
    * @param startService Function to start service on a given port.
    *                     This is expected to throw java.net.BindException on port collision.
    * @param conf A AlohaConf used to get the maximum number of retries when binding to a port.
    * @param serviceName Name of the service.
    * @return (service: T, port: Int)
    */
  def startServiceOnPort[T](
    startPort: Int,
    startService: Int => (T, Int),
    conf: AlohaConf,
    serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = conf.getOption("aloha.port.maxRetries").map(_.toInt).getOrElse(16)
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        userPort(startPort, offset)
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = if (startPort == 0) {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (on a random free port)! " +
                s"Consider explicitly setting the appropriate binding address for " +
                s"the service$serviceString to the correct binding address."
            } else {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (starting from $startPort)! Consider explicitly setting " +
                s"the appropriate port for the service$serviceString " +
                s"to an available port or increasing aloha.port.maxRetries."
            }
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          if (startPort == 0) {
            // As startPort 0 is for a random free port, it is most possibly binding address is
            // not correct.
            logWarning(s"Service$serviceString could not bind on a random free port. " +
              "You may check whether configuring an appropriate binding address.")
          } else {
            logWarning(s"Service$serviceString could not bind on port $tryPort. " +
              s"Attempting port ${tryPort + 1}.")
          }
      }
    }
    // Should never happen
    throw new Exception(s"Failed to start service$serviceString on port $startPort")
  }


  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj:AnyRef):String = {
    obj.getClass.getSimpleName.replace("$","")
  }

  /**
    * Get the ClassLoader which loaded Aloha.
    */
  def getAlohaClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Aloha.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrAlohaClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getAlohaClassLoader)

  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getAlohaClassLoader)
    // scalastyle:on classforname
  }

  /** Determines whether the provided class is loadable in the current thread. */
  def classIsLoadable(clazz: String): Boolean = {
    // scalastyle:off classforname
    Try { Class.forName(clazz, false, getAlohaClassLoader) }.isSuccess
    // scalastyle:on classforname
  }

  /**
    * Create instances of extension classes.
    *
    * The classes in the given list must:
    * - Be sub-classes of the given base class.
    * - Provide either a no-arg constructor, or a 1-arg constructor that takes a AlohaConf.
    *
    * The constructors are allowed to throw "UnsupportedOperationException" if the extension does not
    * want to be registered; this allows the implementations to check the Aloha configuration (or
    * other state) and decide they do not need to be added. A log message is printed in that case.
    * Other exceptions are bubbled up.
    */
  def loadExtensions[T](extClass: Class[T], classes: Seq[String], conf: AlohaConf): Seq[T] = {
    classes.flatMap { name =>
      try {
        val klass = classForName(name)
        require(extClass.isAssignableFrom(klass),
          s"$name is not a subclass of ${extClass.getName()}.")

        val ext = Try(klass.getConstructor(classOf[AlohaConf])) match {
          case Success(ctor) =>
            ctor.newInstance(conf)

          case Failure(_) =>
            klass.getConstructor().newInstance()
        }

        Some(ext.asInstanceOf[T])
      } catch {
        case _: NoSuchMethodException =>
          throw new AlohaException(
            s"$name did not have a zero-argument constructor or a" +
              " single-argument constructor that accepts AlohaConf. Note: if the class is" +
              " defined inside of another Scala class, then its constructors may accept an" +
              " implicit parameter that references the enclosing class; in this case, you must" +
              " define the class as a top-level class in order to prevent this extra" +
              " parameter from breaking Aloha's ability to find a valid constructor.")

        case e: InvocationTargetException =>
          e.getCause() match {
            case uoe: UnsupportedOperationException =>
              logDebug(s"Extension $name not being initialized.", uoe)
              logInfo(s"Extension $name not being initialized.")
              None

            case null => throw e

            case cause => throw cause
          }
      }
    }
  }


  /**
    * Whether the underlying operating system is Windows.
    */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
    * Whether the underlying operating system is Mac OS X.
    */
  val isMac = SystemUtils.IS_OS_MAC_OSX


  def checkHost(host: String, message: String = "") = {
    assert(host.indexOf(':') == -1,message)
  }

  /**
    * Returns the user port to try when trying to bind a service. Handles wrapping and skipping
    * privileged ports.
    */
  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }

  /**
    * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
    */
  private lazy val localIpAddress: InetAddress = findLocalInetAddress()

  private def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("ALOHA_LOCAL_IP")
    if (defaultIpOverride != null) {
      InetAddress.getByName(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " +
              strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
            logWarning("Set ALOHA_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set ALOHA_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }

  private var customHostname: Option[String] = sys.env.get("ALOHA_LOCAL_HOSTNAME")

  /**
    * Allow setting a custom host name because when we run on Mesos we need to use the same
    * hostname it reports to the master.
    */
  def setCustomHostname(hostname: String) {
    // DEBUG code
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  /**
    * Get the local machine's FQDN.
    */
  def localCanonicalHostName(): String = {
    customHostname.getOrElse(localIpAddress.getCanonicalHostName)
  }

  /**
    * Get the local machine's hostname.
    */
  def localHostName(): String = {
    customHostname.getOrElse(localIpAddress.getHostAddress)
  }

  /**
    * Get the local machine's URI.
    */
  def localHostNameForURI(): String = {
    customHostname.getOrElse(InetAddresses.toUriString(localIpAddress))
  }

  def checkHost(host: String) {
    assert(host != null && host.indexOf(':') == -1, s"Expected hostname (not IP) but got $host")
  }

  def checkHostPort(hostPort: String) {
    assert(hostPort != null && hostPort.indexOf(':') != -1,
      s"Expected host and port but got $hostPort")
  }

  /**
    * Split the comma delimited string of master URLs into a list.
    * For instance, "aloha://abc,def" becomes [aloha://abc, aloha://def].
    */
  def parseStandaloneMasterUrls(masterUrls: String): Array[String] = {
    masterUrls.stripPrefix("aloha://").split(",").map("aloha://" + _)
  }
}
