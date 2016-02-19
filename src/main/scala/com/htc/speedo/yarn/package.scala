/*
 * Copyright 2016 HTC Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.htc.speedo

import java.io.File
import java.net.InetAddress
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MutableMap }
import scala.util.Try
import scala.xml.XML

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.yarn.api.ApplicationConstants.{ Environment, LOG_DIR_EXPANSION_VAR }
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.util.{ Apps, ConverterUtils, Records }

/**
 * helper function for build Container/AppMaster Context info
 *
 * @author Wenrui Jiang (roy_jiang@htc.com)
 */
package object yarn {
  /** size unit */
  val gB = 1024
  /**
   * JVM heap size proportion of yarn container memory size.
   * YARN only pays attention to the amount of physical memory used by a
   * process. With Java, you can set the heap, but there's also permgen, JVM,
   * JNI libraries, and off-heap memory usage. All of these contribute to the
   * physical memory usage that YARN cares about, but are outside the JVM heap
   */
  val heapProportion = .6f
  /** check waiting time */
  val waitTime = 1000

  lazy val AppConfig = XML.load(getClass.getResource("/com/htc/speedo/yarn/AppConf.xml"))
  private def getEle(label: String) = AppConfig.\\(label).text

  /** get AppMaster configuration from AppConf.xml */
  lazy val appMem = getEle("appMemory").toInt
  lazy val appCores = getEle("appCores").toInt
  lazy val appName = getEle("appName")
  lazy val queueType = getEle("queueType")

  /** get Container configuration from AppConf.xml */
  lazy val containerMem = getEle("containerMemory").toInt
  lazy val containerCores = getEle("containerCores").toInt
  lazy val priorityLevel = getEle("priorityLevel").toInt
  lazy val javaOpts = getEle("javaOpts")

  /** The YarnApp class name for the application. */
  val AppClassFlag = "appClass"
  /** The Yarn application id for the application. */
  val AppIdFlag = "appId"
  /** resource flag indicating memory requirement for each container */
  val coreFlag = "core"
  /** resource flag indicating vCore requirement for each container */
  val memFlag = "mem"
  /** resource flag indicating heap size requirement for the job */
  val heapFlag = "heap"
  /** resource flag indicating host launch the application master container */
  val amFlag = "appMaster"

  /** get local host name */
  def getHostName: String =
    Try(InetAddress.getLocalHost().getHostName).toOption
      .filter(StringUtils.isNotEmpty)
      .orElse(Option(System.getenv("HOSTNAME"))) // Unix
      .orElse(Option(System.getenv("COMPUTERNAME"))) // Windows
      .getOrElse("localhost")

  /** build command to launch a JVM */
  def launchJVM(mainClass: String, arguments: List[String]): List[String] =
    List("hadoop", mainClass) ++ arguments ++ List(
      s"1>$LOG_DIR_EXPANSION_VAR/stdout", s"2>$LOG_DIR_EXPANSION_VAR/stderr"
    )

  /**
   * build [[ContainerLaunchContext]] which defines all informations to launch an container.
   * inluding:
   *  - container id
   *  - the command to be executed
   *  - the local resources (binaries, jars, files etc.)
   *  - security tokens
   *  - environment settings (CLASSPATH etc.)
   */
  def buildContainerContext(cmd: List[String], hdfsPaths: List[Path],
    env: MutableMap[String, String], tokens: Option[ByteBuffer] = None)(
    implicit
    conf: Configuration
  ): ContainerLaunchContext = {
    val appMasterEnv = hdfsPaths.map { hdfsPath =>
      val appMasterJar = Records.newRecord(classOf[LocalResource])
      setUpLocalResource(hdfsPath, appMasterJar)
      (hdfsPath.getName, appMasterJar)
    }.toMap.asJava
    val container = Records.newRecord(classOf[ContainerLaunchContext])
    if (tokens.isDefined) container.setTokens(tokens.get.duplicate)
    container.setCommands(cmd.asJava)
    container.setLocalResources(appMasterEnv)
    container.setEnvironment(env.asJava)
    container
  }

  /**
   * add the jar which contains the Application master code to local resource
   *
   * @note using the LocalResource to add resources to our application request will cause YARN to
   * distribute application's jars to all of the nodes in the YARN cluster that need it
   */
  def setUpLocalResource(resourcePath: Path, res: LocalResource)(
    implicit
    conf: Configuration
  ): Unit = {
    val jarStat = FileSystem.get(conf).getFileStatus(resourcePath)
    res.setResource(ConverterUtils.getYarnUrlFromPath(resourcePath))
    res.setSize(jarStat.getLen)
    res.setTimestamp(jarStat.getModificationTime)
    res.setType(LocalResourceType.FILE)
    res.setVisibility(LocalResourceVisibility.PUBLIC)
  }

  /**
   * Add Environment
   * @param env The mutable map that contains environment variables
   * @param jarNames Additional jars appended to HADOOP_CLASSPATH
   * @param heapSize Heap size in gb, use None for default, e.g. 2
   */
  def setUpEnv(env: MutableMap[String, String], jarNames: List[String],
    heapSize: Float): Unit = {
    jarNames.foreach { Apps.addToEnvironment(env.asJava, "HADOOP_CLASSPATH", _, File.pathSeparator) }
    val heapInMB = (heapSize * gB).toInt
    Apps.addToEnvironment(env.asJava, "JAVA_HEAP_MAX", "-Xmx" + heapInMB + "M", File.pathSeparator)
  }
}
