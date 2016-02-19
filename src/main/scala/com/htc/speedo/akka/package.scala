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

import _root_.akka.actor.ActorRef

import com.twitter.scalding.Args

package object akka {
  /** Name of the actor system. */
  val SystemName = "SpeeDO"

  /** The command line option of max train iteration in master actors. */
  val MaxIterFlag = "maxIter"
  /** The command line option of first iteration number in master actors. */
  val StartIterFlag = "startIter"

  /**
   * A pinned dispatcher for all actors that uses [[CaffeWorker]] to make sure the actor is always
   * running on the same thread. This is required for GPU related codes, since the GPU device is set
   * per thread.
   */
  val WorkerDispatcher = "worker-dispatcher"

  /** The message for an akka system to join. Used in HostActor */
  case object Join
  /** The message for time out is triggered. */
  case object JoinTimeOut
  /** The message to stop. */
  case object StopAkka

  /** The message to forward message, used in db actor to keep message order. */
  case class Forward(worker: ActorRef, message: Any)
  /** The message to query and return current progress. */
  case class Progress(progress: Float)
  /** The message that indicates the training is finished. */
  case object TrainFinished
  /** Update training parameters of the caffe worker from command line. */
  case class UpdateParameter(arg: Args)
  /** The message for created worker actor after master actor started */
  case class WorkerCreated(worker: ActorRef)
  /** The message to inform DB actor about keys of all active worker actors */
  case class UpdateWorkers(keys: Set[String])

  /** The message to init caffe snapshot. */
  case class Init(resume: Boolean)
  /** The message to clear the corresponding suffix key of the given worker. */
  case class ClearWorkerKey(worker: ActorRef)
  /** The message to train caffe as the given java iteration. */
  case class Train(iteration: Int)
  /**
   * The message to merge snapshots written by the worker, used in db actor.
   * @param worker The worker to merge snapshot from.
   * @param silent If set to true, will not warn if the delta is empty. This should only be used
   * together with drop mater actor. Default is false.
   */
  case class Merge(worker: ActorRef, silent: Boolean = false)
  /** The message to merge snapshots from all worker, used in db actor. */
  case object MergeAll
  /** The message to represent one train is finished. */
  case class Trained(loss: Double)
  /** The message to test caffe once. */
  case object Test
  /** The message of test accuracy. */
  case class TestResult(accuracy: Option[Double])

  /**
   * The message to query current progress, should only used between [[HybridMasterActor]] and
   * [[MasterActor]]. Progress is calculated as `(count + offset - maxIter) / base` in master actor.
   */
  private[akka] case class ProgressIter(offset: Int, base: Int)
}
