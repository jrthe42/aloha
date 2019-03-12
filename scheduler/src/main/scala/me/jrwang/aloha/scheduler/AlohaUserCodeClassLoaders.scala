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

package me.jrwang.aloha.scheduler

import java.io.IOException
import java.net.{URL, URLClassLoader}

import scala.collection.mutable.ListBuffer

object AlohaUserCodeClassLoaders {
  val DEFAULT_ALWAYS_PARENT_FIRST_PATTERNS = Array(
    "java.", "scala.", "me.jrwang.aloha.", "javax.annotation.",
    "org.slf4j", "org.apache.log4j", "org.apache.logging",
    "org.apache.commons.logging", "ch.qos.logback"
  )

  def parentFirst(urls: Array[URL], parent: ClassLoader): URLClassLoader = {
    new ParentFirstClassLoader(urls, parent)
  }

  def childFirst(urls: Array[URL]): URLClassLoader = {
    new ChildFirstClassLoader(
      urls, AlohaUserCodeClassLoaders.getClass.getClassLoader, DEFAULT_ALWAYS_PARENT_FIRST_PATTERNS)
  }

  def childFirst(urls: Array[URL], parent: ClassLoader): URLClassLoader = {
    new ChildFirstClassLoader(urls, parent, DEFAULT_ALWAYS_PARENT_FIRST_PATTERNS)
  }

  def childFirst(urls: Array[URL], alwaysParentFirstPatterns: Array[String]): URLClassLoader = {
    new ChildFirstClassLoader(
      urls, AlohaUserCodeClassLoaders.getClass.getClassLoader, alwaysParentFirstPatterns)
  }

  def childFirst(urls: Array[URL], parent: ClassLoader, alwaysParentFirstPatterns: Array[String]): URLClassLoader = {
    new ChildFirstClassLoader(urls, parent, alwaysParentFirstPatterns)
  }
}

/**
  * Regular URLClassLoader that first loads from the parent and only after that from the URLs.
  */
class ParentFirstClassLoader (val urls: Array[URL], val parent: ClassLoader)
  extends URLClassLoader(urls, parent) {}

/**
  * A variant of the URLClassLoader that first loads from the URLs and only after that from the parent.
  */
class ChildFirstClassLoader(
    val urls: Array[URL],
    val parent: ClassLoader,
    val alwaysParentFirstPatterns: Array[String])
  extends URLClassLoader(urls, parent) {

  @throws[ClassNotFoundException]
  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    // First, check if the class has already been loaded
    var c = findLoadedClass(name)

    if (c == null) {
      // check whether the class should go parent-first
      if(alwaysParentFirstPatterns.exists(name.startsWith)) {
        return super.loadClass(name, resolve)
      }
      try {
        // check the URLs
        c = findClass(name)
      } catch {
        case e: ClassNotFoundException =>
          // let URLClassLoader do it, which will eventually call the parent
          c = super.loadClass(name, resolve)
      }
    }
    if (resolve) {
      resolveClass(c)
    }
    c
  }

  override def getResource(name: String): URL = { // first, try and find it via the URLClassloader
    val urlClassLoaderResource = findResource(name)
    if (urlClassLoaderResource != null) return urlClassLoaderResource
    // delegate to super
    super.getResource(name)
  }

  @throws[IOException]
  override def getResources(name: String): java.util.Enumeration[URL] = {
    // first get resources from URLClassloader
    val urlClassLoaderResources = findResources(name)
    val result: ListBuffer[URL] = ListBuffer[URL]()
    while (urlClassLoaderResources.hasMoreElements) {
      result += urlClassLoaderResources.nextElement()
    }
    // get parent urls
    val parentResources = getParent.getResources(name)
    while (parentResources.hasMoreElements) {
      result += parentResources.nextElement()
    }
    new java.util.Enumeration[URL] {
      val iter:Iterator[URL] = result.iterator

      override def hasMoreElements: Boolean = {
        iter.hasNext
      }

      override def nextElement(): URL = {
        iter.next()
      }
    }
  }
}