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

import java.util.{ List => jList }
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable.{ Map => MutableMap }

import com.twitter.scalding.Args

import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.async.{ AMRMClientAsync, NMClientAsync }
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.utils.BuilderUtils
import org.apache.hadoop.yarn.util.Records
import org.slf4j.LoggerFactory

object AppContainers {
  def main(args: Array[String]): Unit = {
    val appContainers = new AppContainers(args)
    val state = appContainers.run
    if (state) System.exit(0) else System.exit(2)
  }
}

/**
 * Allocate and launch the containers that will run the application job
 *
 * For each allocated container, the <code>ApplicationMaster</code> can then set up the necessary
 * launch context via ContainerLaunchContext to specify the allocated container id, local resources
 * required by the executable, the environment to be setup for the executable, commands to execute,
 * etc. and submit a link StartContainerRequest to the ContainerManagementProtocol to launch and
 * execute the defined commands on the given allocated container.
 *
 * @author Wenrui Jiang (roy_jiang@htc.com)
 */
class AppContainers(gArgs: Array[String]) {
  @transient private val logger = LoggerFactory.getLogger(classOf[AppContainers])
  val sArgs = Args(gArgs)
  val (clusterTimestamp, id) = sArgs.list(AppIdFlag) match {
    /** Combination of clusterTimestamp and id will be an application id */
    case List(clusterTimestamp, id) => (clusterTimestamp.toLong, id.toInt)
    case s: List[String] => throw new IllegalArgumentException("wrong args for application id...")
  }
  val newArgs = new Args(sArgs.m.-(AppClassFlag, coreFlag, memFlag, AppIdFlag, heapFlag))
  val appId = BuilderUtils.newApplicationId(clusterTimestamp, id)

  val appClass = sArgs.required(AppClassFlag)
  /** resources for the container which will launch the Application */
  val cMem = sArgs.float(memFlag, containerMem)
  val cCores = sArgs.int(coreFlag, containerCores)
  val cHeapSize = sArgs.float(heapFlag, cMem * heapProportion)

  implicit val conf = new YarnConfiguration()
  /** start a yarn Client */
  val yarnClient = YarnClient.createYarnClient
  yarnClient.init(conf)
  yarnClient.start

  /**
   * Count of running containers requested from the RM
   * Needed as once requested, we should not request for containers again.
   * Only request for more if the original requirement changes.
   */
  private val numRunningContainers: AtomicInteger = new AtomicInteger()
  /** Count of total containers already requested from the RM. */
  private val totalRequestedContainers: AtomicInteger = new AtomicInteger()
  /** indicate if the application completed and the success or not */
  private var done: Option[Boolean] = None

  /** get application report */
  val appReport = yarnClient.getApplicationReport(appId)

  val hdfs_classpath_root = new Path("/user/" + appReport.getUser + "/yarnapp.staging/" + appId.toString)
  val hdfsPaths = FileSystem.get(conf).listStatus(hdfs_classpath_root).map(_.getPath).toList

  /** get instance of this application */
  val yarnApp = ReflectionUtils.getInstaceFrom(appClass, newArgs).get.asInstanceOf[YarnApp]
  /** total number of containers needed by the application */
  val numTotalContainers: Int = yarnApp.getSize(gArgs)
  /** max number of containers allow to request  */
  val maxRequest: Int = numTotalContainers * 2

  /** Handle to communicate with the Resource Manager */
  val amRMClientAsync: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(1000, RMCallbackHandler)
  amRMClientAsync.init(conf)
  amRMClientAsync.start

  /** Handle to communicate with the Node Manager */
  val nmClientAsync: NMClientAsync = NMClientAsync.createNMClientAsync(null)
  nmClientAsync.init(conf)
  nmClientAsync.start

  /**
   * launch containers for each task, after a container has been allocated to the ApplicationMaster,
   * it needs to set up the ContainerLaunchContext for the eventual task that is going to be running
   * on the allocated Container.
   */
  def run: Boolean = {
    /** Registers this application master client with the resource manager */
    val response = amRMClientAsync.registerApplicationMaster("", 0, "")

    val previousRunningContainers = response.getContainersFromPreviousAttempts
    logger.debug(appReport.getCurrentApplicationAttemptId.toString + " received " + previousRunningContainers.size() +
      " previous attempts' running containers on AM registration.")

    val containerAsk = setupContainerAskForRM(cMem, cCores)
    val numTotalContainersToRequest = numTotalContainers - previousRunningContainers.size
    // request containers from ResourceManager
    (1 to numTotalContainersToRequest).map { _ => amRMClientAsync.addContainerRequest(containerAsk) }
    numRunningContainers.set(numTotalContainers)
    totalRequestedContainers.set(numTotalContainers)

    // wait for completion.
    while (done.isEmpty) {
      try { Thread.sleep(waitTime) } catch { case _: InterruptedException => }
    }

    // When the application completes, it should stop all running containers
    logger.info("Application completed. Stopping running containers")
    nmClientAsync.stop

    // When the application completes, it should send a finish application signal to the RM
    val success = done.getOrElse(false)
    val appStatus = if (success) FinalApplicationStatus.SUCCEEDED else FinalApplicationStatus.FAILED

    amRMClientAsync.unregisterApplicationMaster(appStatus, "", null)
    amRMClientAsync.stop
    yarnClient.stop
    success
  }

  /**
   * Setup the request that will be sent to the RM for the container ask.
   * @return the setup ResourceRequest to be sent to RM
   */
  def setupContainerAskForRM(cMem: Float, cCores: Int): ContainerRequest = {
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(priorityLevel)

    /** resources needed for each container */
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory((cMem * gB).toInt)
    resource.setVirtualCores(cCores)

    new ContainerRequest(resource, null, null, priority)
  }

  object RMCallbackHandler extends AMRMClientAsync.CallbackHandler {
    val masterRole = yarnApp.getApp(gArgs, getHostName)

    override def onContainersCompleted(statuses: jList[ContainerStatus]) = {
      statuses.asScala.foreach { containerStatus =>
        logger.debug("Get container status for containerID=" +
          containerStatus.getContainerId + ", state=" +
          containerStatus.getState + ", exitStatus=" +
          containerStatus.getExitStatus + ", diagnostics=" +
          containerStatus.getDiagnostics)

        containerStatus.getExitStatus match {
          case ContainerExitStatus.SUCCESS =>
            /** Container completed successfully */
            logger.info("Container completed successfully." + ", containerId=" + containerStatus.getContainerId)
          case ContainerExitStatus.ABORTED | ContainerExitStatus.KILLED_EXCEEDED_PMEM | ContainerExitStatus.KILLED_EXCEEDED_VMEM =>
            logger.warn("container aborted or killed(like oom problem)," + "will be recovered later!")
            /**
             * container was killed by framework, possibly preempted we should
             * re-try as the container was lost for some reason, do not need
             * to release the container as it would be done by the RM
             */
            numRunningContainers.decrementAndGet
          // TODO: test for system.exit
          case status: Int if status > 0 =>
            logger.warn("container manually killed, will be recovered later!")
            numRunningContainers.decrementAndGet
          case _ =>
            logger.warn("container failed for unkown reason")
        }
      }
      /** ask for more containers if any failed */
      val askCount = numTotalContainers - numRunningContainers.get
      numRunningContainers.addAndGet(askCount)
      totalRequestedContainers.addAndGet(askCount)
      if (totalRequestedContainers.get > maxRequest) {
        logger.warn("number of total containers requesting exceeds the threshold")
        onShutdownRequest
      } else (1 to askCount).foreach { _ =>
        logger.debug("Append new container request!!")
        val containerAsk = setupContainerAskForRM(cMem, cCores)
        amRMClientAsync.addContainerRequest(containerAsk)
      }
    }

    override def onContainersAllocated(containers: jList[Container]) = {
      logger.debug("Allocated containers " + containers.asScala.map(
        c => c.getId.toString + " on " + c.getNodeId.getHost
      ).mkString(" "))
      /** remove previous container requesting cache */
      containers.asScala.foreach { c =>
        val ask = new ContainerRequest(c.getResource, null, null, c.getPriority)
        amRMClientAsync.removeContainerRequest(ask)
      }

      /** setup application environment */
      val env = MutableMap(masterRole.appEnv.toSeq: _*)
      setUpEnv(env, hdfsPaths.map(_.getName), cHeapSize)
      /** start slave role on corresponding container */
      containers.asScala.foreach { container =>
        val cmd = launchJVM(masterRole.slaveMain, masterRole.slaveArgs(container.getNodeId.getHost))
        val ctx = buildContainerContext(cmd, hdfsPaths, env)
        nmClientAsync.startContainerAsync(container, ctx)
      }
    }

    override def onShutdownRequest() = {
      done = Some(false) // this is called when master is out of sync
      amRMClientAsync.stop
    }

    override def onNodesUpdated(updatedNodes: jList[NodeReport]) = {}

    override def getProgress(): Float = masterRole.action() match {
      case InProgress(progress) => Math.min(Math.max(0, progress), 1)
      case Finished(success) =>
        done = Some(success)
        1f
    }

    override def onError(e: Throwable) = {
      e.printStackTrace
      onShutdownRequest
    }
  }
}
