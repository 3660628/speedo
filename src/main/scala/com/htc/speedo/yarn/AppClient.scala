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

package com.htc.speedo.yarn

import java.io.{ File, FileFilter }
import java.net.URLClassLoader
import java.text.SimpleDateFormat
import java.util.{ Date, TimeZone }

import scala.sys.process._

import com.twitter.scalding.Args

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.fs.permission.{ FsAction, FsPermission }
import org.apache.hadoop.yarn.api.records.{ Resource, YarnApplicationState }
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

/**
 * YARN client which launches an Application and AppMaster by adding the jar to local resources.
 *
 * In need to submit an application, the client needs to provide sufficient information to the
 * ResourceManager to launch the application's first container: ApplicationMaster. This info is
 * named as [[ApplicationSubmissionContext]], which include:
 *  - Application Info: id, name
 *  - Queue info: Queue to which the application will be submitted.
 *  - Priority info: the priority to be assigned for the application.
 *  - User: The user submitting the application
 *  - ContainerLaunchContext, see [[buildContainerContext]]
 *
 * @note This is an entry point for the Yarn application.
 *
 * Ways to run the yarn app:
 *  - hadoop jar executable.jar --appClass <mainClass> [options]
 *  - HADOOP_CLASSPATH=executable.jar hadoop <mainClass> [options]
 *
 * TODO: Support multiple jars
 * @author Wenrui Jiang (roy_jiang@htc.com)
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
object AppClient {

  /** define file filter to keep executable jar package */
  val jarFileFilter = new FileFilter {
    override def accept(file: File) = file.isFile && file.getName.endsWith(".jar")
  }

  /** Remove arguments from command line. */
  def removeArg(args: Array[String], keys: String*): Array[String] = {
    keys.foldLeft(args) { (array, key) =>
      val ind = array.indexOf(s"--$key")
      if (ind > -1)
        array.patch(ind, Nil, 2)
      else
        array
    }
  }

  /** launches the AppMaster with the Application */
  def main(args: Array[String]): Unit = {
    // scalding arguments
    val sArgs = Args(args)
    /** resources for the container which will launch the ApplicationMaster */
    val mMem = sArgs.float(memFlag, appMem)
    val mCores = sArgs.int(coreFlag, appCores)
    val mHeapSize = sArgs.float(heapFlag, mMem * heapProportion)

    // get hadoop classpath from hadoop command
    val hadoop_classpath = Process(Seq("hadoop", "classpath"), None,
      "HADOOP_CLASSPATH" -> "").!!.split(":").map(_.trim).filterNot(_.isEmpty)
      .flatMap { path =>
        val file = new File(path.stripSuffix("*")).getCanonicalFile
        if (file.isFile) Array(file)
        else file.listFiles(jarFileFilter).map(_.getCanonicalFile)
      }

    // get hadoop classpath from runtime environment
    val classpath = Thread.currentThread.getContextClassLoader match {
      case url: URLClassLoader => url.getURLs.map(url => url.getFile).toList
      case _ => sArgs.list("classPath")
    }

    val user_classpath = classpath.map(path => new File(path).getCanonicalFile)
      .filter(jarFileFilter.accept).diff(hadoop_classpath)

    /** create yarn configuration */
    implicit val conf = new YarnConfiguration()

    /** start a yarn client */
    val yarnClient = YarnClient.createYarnClient
    yarnClient.init(conf)
    yarnClient.start

    /** create yarn application */
    val app = yarnClient.createApplication
    val appResponse = app.getNewApplicationResponse
    val appId = appResponse.getApplicationId

    val fs = FileSystem.get(conf)
    // hdfs classpath should contain URI whose scheme and authority
    // identify this FileSystem
    val stagingRoot = new Path(fs.getUri.toString, fs.getHomeDirectory + "/yarnapp.staging")
    val hdfs_classpath_root = new Path(stagingRoot, appId.toString)

    /** upload user classpath package to hdfs */
    fs.mkdirs(hdfs_classpath_root)
    // make sure the directory can be deleted on exit
    fs.setPermission(stagingRoot, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL))
    fs.deleteOnExit(hdfs_classpath_root)
    fs.copyFromLocalFile(false, true, user_classpath.map(file =>
      new Path(file.getCanonicalPath)).toArray, hdfs_classpath_root)

    // TODO: handle duplicate jar names
    val hdfsPaths = user_classpath.map(file => new Path(hdfs_classpath_root, file.getName))

    /** setup env to get all yarn and hadoop classes in classpath */
    val env = collection.mutable.Map[String, String]()
    setUpEnv(env, user_classpath.map(_.getName), mHeapSize)

    /**
     * build [[ContainerLaunchContext]] for the container which will launch the [[ApplicationMaster]]
     */
    val cmd = launchJVM(
      classOf[AppContainers].getName,
      (args :+ (s" --$AppIdFlag ${appId.getClusterTimestamp} ${appId.getId}")).toList
    )
    val ctx = buildContainerContext(cmd, hdfsPaths, env)

    /**
     * Set the resource required by the [[ApplicationMaster]]
     * for this application
     */
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory((mMem * gB).toInt)
    resource.setVirtualCores(mCores)

    /**
     * setup the [[ApplicationSubmissionContext]] which defines all the
     * information needed by the ResourceManager to launch the
     * [[ApplicationMaster]].
     */
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName(appName)
    appContext.setAMContainerSpec(ctx)
    appContext.setResource(resource)
    appContext.setQueue(queueType)

    val clockF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val elapsedF = new SimpleDateFormat("HH:mm:ss.SSS")
    elapsedF.setTimeZone(TimeZone.getTimeZone("GMT+0"))
    /** submit the application */
    println("submitting application " + yarnClient.submitApplication(appContext) +
      " Clock: " + clockF.format(new Date(System.currentTimeMillis)))

    /** init yarn application report */
    var appReport = yarnClient.getApplicationReport(appId)
    val start_time = appReport.getStartTime
    var appState = appReport.getYarnApplicationState

    sys.addShutdownHook {
      if (appState == YarnApplicationState.RUNNING ||
        appState == YarnApplicationState.ACCEPTED)
        yarnClient.killApplication(appId)

      val fTime = appReport.getFinishTime
      println(appId + " last state: " + appState + " Clock: " +
        clockF.format(new Date(if (fTime > 0) fTime else System.currentTimeMillis)))

      yarnClient.stop
    }

    while (appState != YarnApplicationState.FINISHED &&
      appState != YarnApplicationState.KILLED &&
      appState != YarnApplicationState.FAILED) {
      try {
        // get running status every minute
        Thread.sleep(60000)
        appReport = yarnClient.getApplicationReport(appId)
        appState = appReport.getYarnApplicationState
        println("%s running : %4.2f%%, time elapse(ms): %s".format(
          appId, appReport.getProgress * 100, elapsedF.format(new Date(System.currentTimeMillis - start_time))
        ))
      } catch { case _: InterruptedException => }
    }
  }
}
