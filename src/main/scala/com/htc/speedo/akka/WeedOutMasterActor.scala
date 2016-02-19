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

import scala.collection.mutable.Buffer

import akka.actor.ActorRef

import com.twitter.scalding.Args

import MasterActor._

/**
 * Weed-out strategy that discard deltas from delayed workers. A delayed worker is determined by an
 * interval. We use a fix sized queue to record the finished workers of last `interval` iterations.
 * When a worker finished training, we define it is not delayed i.i.f it exists in the queue. No
 * matter the worker is delayed or not, it's still enqueued and start a new training
 * (after the merge if not delayed).
 * Required parameter:
 *  - `--weedout`: The interval to determine if a worker is delayed or not. Must be at least same as
 * size of workers.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
trait WeedOutMasterActor extends MasterActor {
  /** The interval to determine weedout, at least size of workers. */
  var maxInterval = Math.max(args.required("weedout").toInt, workers.size)

  /** An fix sized queue to hold last updated workers. */
  val lastUpdates = Buffer[ActorRef]()

  /** The next index to update in the [[lastUpdates]] queue. */
  var updateIndex = 0

  override def strategyName = "weed-out"

  override def parseTrainResult(loss: Double) = {
    val needMerge =
      if (lastUpdates.size < maxInterval) {
        // For the first few iterations, always do merge
        lastUpdates += sender
        true
      } else {
        // If the sender exists in [[lastUpdates]], then we consider it not delay
        // and merge its delta into snapshot weight
        val merge = lastUpdates.contains(sender)
        // update the last updated workers in the queue
        lastUpdates(updateIndex) = sender
        merge
      }
    // update the next index in queue
    updateIndex += 1
    if (updateIndex == maxInterval) updateIndex = 0
    // always start training for the worker
    ParsedTrainResult(if (needMerge) MergeResultSender else MergeResultNone)
  }

  override def workerCreated(worker: ActorRef) = {
    lastUpdates.insert(updateIndex, worker) // insert worker as oldest updater
    updateIndex += 1 // update next index
    maxInterval += 1 // the interval is increaed by 1
    super.workerCreated(worker) // start training
  }

  override def workerTerminated(worker: ActorRef) = {
    // remove oldest element
    if (updateIndex < lastUpdates.size) lastUpdates.remove(updateIndex, 1)
    maxInterval -= 1 // the interval is removed by 1
    if (updateIndex == maxInterval) updateIndex = 0 // update next index
  }
}
