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

import akka.actor.{ Actor, ActorLogging, ActorRef, Terminated }

import com.twitter.scalding.Args

import MasterActor._

/**
 * The master actor, which is responsible to organize how workers cooperate. By default, it acts as
 * asynchrounous master actor.
 * @param args Command line arguments used to initialize master actor and caffe worker.
 * Parameters required by master actor:
 *  - '--maxIter': The maximum number of caffe runs, must be provided.
 *  - '--startIter': The number of first iteration, default is 0. This is useful if the training is
 * continued from previous snapshots. Start iteration is only used in logging and setting iteration
 * and learning rate for caffe worker.
 *  - '--test': The iteration interval to trigger tests. Default is maxIter. If set to 0, then all
 * tests are skipped by test actor.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class MasterActor(args: Args, dbActor: ActorRef, tester: ActorRef, workers: Buffer[ActorRef])
    extends Actor with ActorLogging {
  // output arguments
  log.info("Running {} master actor with args: {}", strategyName, args.toList.mkString(" "))

  // watch all workers
  workers.foreach(context.watch)

  /** The start iteration, i.e. iterations trained before this master actor */
  val startIter = args.int(StartIterFlag, 0)
  /** The maximum iterations to invoke workers for training. */
  val maxIter = args.int(MaxIterFlag)
  /** The iteration interval to trigger test repeatedly. */
  val testInterval = args.int("test", maxIter)

  /** The current iterations run. */
  var count = 0

  /** The name of parallel strategy in this master actor, used in logging */
  def strategyName: String = "asynchrounous"

  override def receive = commonState orElse {
    case Init =>
      log.info("DB actor initialized snapshot")
      // run test after initialization
      tester ! Test
      // start training
      workers.foreach(_ ! trainMessage)
      // Change behavior of this actor (Support train and test results)
      context.become(trainState)
  }

  /** The common message that are safe for all states. */
  val commonState: Receive = {
    case Progress => sender ! Progress(count.toFloat / maxIter)
    case ProgressIter(offset, base) => sender ! Progress((count + offset - maxIter) / base.toFloat)
    case TestResult(Some(accuracy)) => log.info("Current accuracy is {}", accuracy)
    case TestResult(None) => log.warning("Test failed!")
  }

  /** The message parsing for training and testing results. */
  def trainState: Receive = commonState orElse {
    case Trained(loss) =>
      val ParsedTrainResult(needMerge, needTrain) = parseTrainResult(loss)
      needMerge match {
        case MergeResultNone =>
          log.info("Dropped {} with loss = {}.", sender.path.name, loss)
          // clear worker's suffix key, so we merge nothing
          dbActor ! ClearWorkerKey(sender)
          // we still need to merge here, since we need to make sure the next
          // training can read the latest snapshot, skip warning
          dbActor ! Merge(sender, true)
        case MergeResultSender =>
          count = count + 1
          log.info("{} finished {}th run with loss = {}.", sender.path.name, count + startIter, loss)
          // merge the delta into snapshot
          dbActor ! Merge(sender)
        case MergeResultWait =>
          log.info("{} finished part of batch with loss = {}, waiting to merge", sender.path.name, loss)
        case MergeResultAll(aveloss) =>
          count = count + 1
          log.info("{} finished {}th run with loss = {} (averaged), {} (raw)", sender.path.name, count + startIter, aveloss, loss)
          dbActor ! MergeAll
      }
      if (count >= maxIter) {
        dbActor ! Forward(context.parent, TrainFinished)
        // unwatch workers, so as to avoid DeathPactException
        workers.foreach(context.unwatch)
        // Do not merge further training reslts
        context.become(commonState)
      } else {
        needTrain match {
          case StartTrainNone => // do nothing
          case StartTrainSender =>
            dbActor ! Forward(sender, trainMessage)
          case StartTrainAll =>
            workers.foreach(dbActor ! Forward(_, trainMessage))
        }
        if (testInterval > 0 && count % testInterval == 0 &&
          // Don't trigger test if we didn't merge deltas
          needMerge != MergeResultWait && needMerge != MergeResultNone) {
          log.info("Start testing")
          dbActor ! Forward(tester, Test)
        }
      }
    case WorkerCreated(worker) =>
      workers += worker
      context.watch(worker)
      workerCreated(worker)
    case Terminated(worker) if workers.contains(worker) =>
      workers -= worker
      workerTerminated(worker)
  }

  /**
   * Override this function in the subclasses for implementing different parallel strategies.
   * @return [[ParsedTrainResult]] with two options: [[MergeResult]] and [[StartTrain]]. Default is
   * [[MergeResultSender]] and [[StartTrainSender]].
   * @note [[count]] will be increased if merge.
   */
  def parseTrainResult(loss: Double): ParsedTrainResult = ParsedTrainResult()

  /**
   * A handler for new worker actors created after the master actor is started.
   *
   * The default implementation (for asynchronous master actor) just send a Train message to the
   * new worker.
   * @note The worker is already added to the [[workers]] buffer and watched by the master actor.
   */
  def workerCreated(worker: ActorRef): Unit = worker ! trainMessage

  /**
   * A handler for terminated worker actors during master actor's run.
   *
   * The default implementation (for asynchronous master actor) does nothing.
   * @note The worker is already removed from the [[workers]] buffer.
   */
  def workerTerminated(worker: ActorRef): Unit = {}

  /**
   * A helper function to create a [[Train]] message, according to the current
   * iteration and start iteration.
   */
  final def trainMessage: Train = Train(count + startIter)
}

object MasterActor {
  /** The base trait for a enumeration of how to merge the trained result. */
  sealed trait MergeResult
  /** Do not merge the result from the sender. */
  case object MergeResultNone extends MergeResult
  /** Merge the result from the sender. */
  case object MergeResultSender extends MergeResult
  /**
   * Do not merge the result from the sender now, but output log are same as [[MergeResultSender]].
   * @note Only used for synchronous master actor. And the result will be merged
   * when [[MergeResultAll]] is triggered.
   */
  case object MergeResultWait extends MergeResult
  /**
   * Averages results from all workers and merge once.
   * @param aveloss The averaged loss for all the workers.
   * @note Only used for synchronous master actor.
   */
  case class MergeResultAll(aveloss: Double) extends MergeResult

  /** The base trait for a enumeration of how to start the workers to train. */
  sealed trait StartTrain
  /** Do not start he sender to train next iteration. */
  case object StartTrainNone extends StartTrain
  /** Start the sender to train next iteration (after merge if any). */
  case object StartTrainSender extends StartTrain
  /**
   * Start all the senders to train next iteration (after merge if any).
   * @note Should be used with [[StartTrainNone]] to make sure the worker is not queued by more than
   * one train message.
   */
  case object StartTrainAll extends StartTrain

  /**
   * The result type of the abstract [[MasterActor.parseTrainResult]] function to tell the how to
   * act with the result sent from a worker (sender).
   * By default, always merge from sender and start next train for sender.
   */
  case class ParsedTrainResult(merge: MergeResult = MergeResultSender, train: StartTrain = StartTrainSender)
}
