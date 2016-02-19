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

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

import _root_.akka.pattern.ask
import _root_.akka.util.Timeout

import com.twitter.scalding.Args

import com.htc.speedo.akka.{ AkkaUtil, Progress }
import com.htc.speedo.yarn.{ Finished, InProgress, MasterRole, YarnApp }

/**
 * The yarn app to run SpeeDO
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class SpeeDOApp(args: Args) extends YarnApp {
  override def getSize(arg: Array[String]) =
    args.optional("worker").map(_.toInt match {
      case i: Int if i < 1 =>
        throw new IllegalArgumentException("--worker must be positive")
      case i: Int => i
    }).getOrElse(1)

  override def getApp(arg: Array[String], master: String) = SpeeDOMasterRole(args, master)
}

object SpeeDOMasterRole {
  /** The interval to query progress from master actor, in milliseconds. */
  val ProgressQueryInterval = 15000L // 15 seconds
}

/**
 * The master role that defines how to run SpeeDO on yarn.
 * @param args The command line arguments passed to the master.
 * @param master The host name or ip of the master container.
 */
case class SpeeDOMasterRole(args: Args, master: String) extends MasterRole {
  // operations before starting slaves
  /** The actor system for master container. */
  val system = AkkaUtil.createSystem(args + ("host" -> Seq(master)))
  /** The remote address of the [[system]]. */
  val address = AkkaUtil.addressOf(system)
  /** The host actor of [[system]], by default, join time out is 90 seconds. */
  val hostActor = AkkaUtil.createHostActor(
    args + ("timeout" -> Seq(args.getOrElse("timeout", "90"))) +
      // if don't run tests, wait 15 seconds after train for correct exit state
      ("sleepAfterFinish" -> (if (args.optional("test") == Some("0")) Seq("15") else Nil)), system
  )
  /** The full external uri of [[hostActor]]. */
  val path = hostActor.path.toSerializationFormatWithAddress(address)

  override val slaveMain = AkkaUtil.getClass.getName.stripSuffix("$")

  override def slaveArgs(host: String) = List("--host", host, "--master", path)

  /** Current progress of the akka system */
  var progress = 0f

  /** The time of last progress query */
  var lastProgressTime = 0L

  // operations after starting slaves
  override def action = () => {
    val current = System.currentTimeMillis
    if (current - lastProgressTime > SpeeDOMasterRole.ProgressQueryInterval) {
      lastProgressTime = current
      implicit val timeout = Timeout(5.seconds)
      if (system.isTerminated) Finished(progress >= 1f)
      else {
        val future = hostActor ? Progress
        Try(Await.result(future.mapTo[Progress], timeout.duration))
          .toOption.foreach(p => progress = p.progress)
        // if the actor is stopped, but system not yet, return last progress
        InProgress(progress)
      }
    } else InProgress(progress)
  }

  override val waitAllContainers = false
}
