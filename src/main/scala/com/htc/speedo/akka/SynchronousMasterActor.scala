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

import scala.collection.mutable.{ Buffer, Set => MutableSet }

import akka.actor.ActorRef

import com.twitter.scalding.Args

import MasterActor._

/**
 * The implementation of synchronous master actor. The master will wait for all the workers to
 * complete their training before merge all the deltas. The deltas are averaged first and merge into
 * the weights.
 * Required parameter:
 *  - `--sync`: A flag to determine use synchronous master actor.
 * TODO: Support auto adjustment of batch size
 * @note Supports the `--gradientOnly` flag.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
trait SynchronousMasterActor extends MasterActor {
  /** The mutable set containing current waiting workers. */
  val waitingSet = MutableSet(workers: _*)

  /** The mutable set containing all the workers that waits for merging. */
  val mergeSet = MutableSet[ActorRef]()

  /**
   * The immutable set containing the last merge list. This is smae with the worker list in
   * [[DBActor]]. If mergeSet is different with this list, the worekr list in [[DBActor]] is updated
   * by [[UpdateWorkers]] message.
   */
  var lastUpdateWorkers = Set[ActorRef]()

  /** The buffer containint all the losses. */
  val lossList = Buffer[Double]()

  override def strategyName = "synchronous"

  override def parseTrainResult(loss: Double) = {
    // remove the sender from waiting list
    waitingSet -= sender
    if (loss >= 0) { // if the train is not faked by [[workerTerminated]]
      mergeSet += sender
      lossList += loss
    }
    if (waitingSet.isEmpty) {
      waitingSet ++= workers
      val average = lossList.sum / lossList.size
      if (mergeSet != lastUpdateWorkers) {
        lastUpdateWorkers = mergeSet.toSet // to immutable set
        dbActor ! UpdateWorkers(lastUpdateWorkers.map(_.path.name))
      }
      mergeSet.clear
      lossList.clear
      ParsedTrainResult(MergeResultAll(average), StartTrainAll)
    } else ParsedTrainResult(MergeResultWait, StartTrainNone)
  }

  // don't need to do anything, just wait for next iteration
  override def workerCreated(worker: ActorRef) = {}

  // just remove the worker from waiting list
  override def workerTerminated(worker: ActorRef) = {
    waitingSet -= worker
    // fakes train is finished to trigger normal progress of finished iteration
    if (waitingSet.isEmpty) self.tell(Trained(-1), worker)
  }
}
