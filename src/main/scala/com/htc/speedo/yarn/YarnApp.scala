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

/**
 * trait for creating appointed application on Yarn, application can be an Akka system for example.
 */
trait YarnApp {
  /** get the number of containers to launch */
  def getSize(args: Array[String]): Int
  /**
   * define the appointed application, including both what the master and slave roles will do.
   * @param args Command line argument
   * @param master Host name of master container
   */
  def getApp(args: Array[String], master: String): MasterRole
}

/** trait defined the appointed application */
trait MasterRole {
  /** If we should wait all containers to stop or kill them immediately after master is finished. */
  val waitAllContainers: Boolean = true
  /**
   * Command line arguments for launching slave roles.
   * @param host host name for slave container
   */
  def slaveArgs(host: String): List[String]
  /** main class for launching the slave roles */
  val slaveMain: String
  /** define environment for this application system. For master and slaves. */
  val appEnv: Map[String, String] = Map()
  /**
   * Action of the master role after all the slaves are created. If you need to do actions before
   * starting all the slaves (or even before slaveArgs is determined), do it in the constructor.
   * @return A function that will always return [[JobState]], which will be running progress if job
   * not completed, success state if job completed.
   * @note called once per second
   */
  def action: () => JobState
}

/** Job running progress or last state */
sealed trait JobState

/** Job running progress, should be within [0, 1] */
case class InProgress(progress: Float) extends JobState

/** Job final completion state */
case class Finished(success: Boolean) extends JobState
