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

package com.htc.speedo.akka

import akka.actor.{ Actor, ActorIdentity, ActorLogging, Identify }

import com.twitter.scalding.Args

/**
 * A host slave actor is used to connect master roles on yarn. It tells host master actor its
 * address, so the host master actor can create worker remotely on this slave host. The host slave
 * actor then waits until a stop message is received, which indicates the training is finished.
 *
 * Required parameters:
 * - `--master <master host/ip>`: The host name or ip of the master actor system to connect to.
 * The host name or ip must be the same with that passed
 * - `--worker <Number of workers>`: (Optional) How many worker actors will be started in the
 * system, default to 1. Each worker can utilize a different GPU.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class HostSlaveActor(args: Args) extends Actor with ActorLogging {
  /**
   * The path of host master actor. Must be a full remote path.
   * For example, `akka.tcp://DeepLearning@cloud-master:40357/user/host`.
   */
  val masterPath = args.required("master")

  /** Number of workers to start in this system. */
  val workerCount = args.int("worker", 1)

  // Tries to identify if the master exists
  context.actorSelection(masterPath) ! Identify(masterPath)

  override def receive = {
    // If found master actor, join
    case ActorIdentity(`masterPath`, Some(master)) =>
      // Each join message will create a worker actor
      (1 to workerCount).foreach(_ => master ! Join)
    // If not found master actor, log and exit
    case ActorIdentity(`masterPath`, None) =>
      log.error(s"Cannot found master at $masterPath, stopping!")
      context.system.shutdown
    // stop slave akka system
    case StopAkka => context.system.shutdown
  }
}
