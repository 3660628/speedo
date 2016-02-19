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

import scala.collection.mutable.{ Map => MutableMap, Set => MutableSet }

import akka.actor.ActorRef

import com.twitter.scalding.Args

import MasterActor._

/**
 * The Partial Synchronous Actor, which is responsible to start worker actors and collect
 * information. In PS, there're two possible running status for caffe workers, one is normal
 * running status while the other is catup-up status. Fastest/slowest workers not allowed to
 * drift > maxAdvance iterations apart, otherwise, the fastest workers will suspend.
 *
 * Required parameter:
 *  - `--maxAdvance`: The max iteration interval between different workers
 *
 * @author Wenrui Jiang (roy_jiang@htc.com)
 */
trait PSMasterActor extends MasterActor {
  /** The max iteration interval allowed between workers */
  val maxAdvance = args.required("maxAdvance").toInt

  /** all caffe workers and their running iterations */
  val workerIters = MutableMap(workers.map(_ -> 1): _*)

  /** works needing to catch up the fastest worker */
  val catchupWorkers = MutableSet[ActorRef]()

  /**
   * a catch-up flag indicates the running status, false means a normal running status,
   * while true means catch-up.
   */
  var catchup = false

  override def strategyName = "psc"

  override def parseTrainResult(loss: Double) = {
    if (catchup) {
      catchupWorkers -= sender
      // if in catch-up status and the catchupWorkers is empty
      if (catchupWorkers.isEmpty) {
        catchup = false
        log.info("Back to normal running status...")
        workers.foreach(workerIters.update(_, 1))
        ParsedTrainResult(train = StartTrainAll)
      } else ParsedTrainResult(train = StartTrainNone)
    } else { // if in normal running status
      val lastIter = workerIters.get(sender).get
      workerIters.update(sender, lastIter + 1)
      val values = workerIters.values
      catchup = values.max - values.min >= maxAdvance
      if (catchup) {
        catchupWorkers.clear
        catchupWorkers ++= workers
        catchupWorkers -= sender
        log.info("Change to catchup status, advanced actor: {}", sender.path.name)
      }
      ParsedTrainResult(train = if (catchup) StartTrainNone else StartTrainSender)
    }
  }

  override def workerCreated(worker: ActorRef) = {
    // if we are catching up, we do nothing and wait until catch up is over
    if (!catchup) {
      // set current iteration to quickest worker
      workerIters.update(worker, workerIters.values.max)
      super.workerCreated(worker) // start training
    }
  }

  override def workerTerminated(worker: ActorRef) = {
    // clean-up for the worker
    workerIters -= worker
    if (catchup) {
      // if we are catching up, we need to check if we are waiting for worker
      catchupWorkers -= worker
      if (catchupWorkers.isEmpty) { // all other workers are finished
        catchup = false
        log.info("Back to normal running status...")
        workers.foreach { w =>
          workerIters.update(w, 1)
          dbActor ! Forward(w, trainMessage)
        }
      }
    }
  }
}
