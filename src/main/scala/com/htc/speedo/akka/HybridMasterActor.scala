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

import akka.actor.{ Actor, ActorLogging, ActorRef, PoisonPill, Terminated }

import com.twitter.scalding.Args

object HybridMasterActor {
  /**
   * Parse the command line arguments in HybridMasterActor for specifying a sequence of master
   * actors. In the arguments, all leading `--` should be passed in as `=`, and arguments for
   * different master actors are seperated by `==`.
   */
  def parseArgs(args: List[String]): List[Args] = parseArgs(args, Nil).reverse

  /**
   * The recursive implementation of [[parseArgs]].
   * @param args Remaining args
   * @param list Existing list
   * @note The list is in reverse order, i.e. new args are prepended to the list
   */
  @annotation.tailrec
  private def parseArgs(args: List[String], list: List[Args]): List[Args] = {
    // split the sequence by the first `=`, `==`, `===` or etc
    val (prefix, suffix) = args.span(!_.matches("^=+$"))
    // replace all prefixing = to --
    val new_list = Args(prefix.map(_.replaceAll("^=+", "--"))) :: list
    suffix match {
      // if no `==` exists
      case Nil => new_list
      // the first element is `==`, remove it
      case _ :: tail => parseArgs(tail, new_list)
    }
  }
}

/**
 * A HybridMasterActor allows to switch among a sequence of master actors by current iterations.
 *
 * Required Arguments:
 *  - `--hybrid`: The sequence of arguments passed to the underlying master actors. In the arguments,
 * all leading `--` should be passed in as `=`, and arguments for different master actors are
 * seperated by `==`.
 *
 * Optional Arguments:
 *  - '--maxIter': The maximum number of caffe runs. Default is the sum of `maxIter` for all masters.
 * If `--maxIter` is smaller than total `maxIter` for all master actors, then only part of the
 * master sequence is executed; if larger, the master sequence is repeated.
 * See below example for details.
 *
 * Common arguments for all masters, which are not used directly by [[HybridMasterActor]]. They are
 * passed to all master actors along with the arguments given in the `--hybrid` option. If same
 * option is given in both places, the `--hybrid` option has higher priority.
 *  - `--test`: The test interval.
 *
 * Other arguments consumed by master actors should be passed in the `--hybrid` options, since they
 * are not shared by all master actors.
 * @note Do not set `--startIter` in the `--hybrid` argument, as it's always overridden by
 * [[HybridMasterActor]].
 * @example An example input arguments of [[HybridMasterActor]] of
 * `--maxIter 800 --test 50 --hybrid =maxIter 100 =sync == =maxIter 200 =test 100 == =maxIter 300 =drop 3`
 * is interpreted as a sequence of 3 master actors with different parameters. Since the `--maxIter`
 * option is given, is larger than the sum of all master actors, their will be total of 5 master
 * actors running in the following order, with a total of 800 iterations:
 *  - `--maxIter 100 --sync --test 50`: first master, synchronous for 100 iterations, test interval is 50
 *  - `--maxIter 200 --test 100`: second master, asynchronous for 200 iterations, test interval is 100
 *  - `--maxIter 300 --drop 3 --test 50`: third master, drop for 300 iterations, test interval is 50
 *  - `--maxIter 100 --sync --test 50`: now back to first master again, synchronous for 100
 * iterations, test interval is 50
 *  - `--maxIter 100 --test 100`: second master again, asynchronous for the last 100 iterations,
 * test interval is 100. Only run 100 iterations, instead of 200, since it exceeds `--maxIter`.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class HybridMasterActor(args: Args, db: ActorRef, tester: ActorRef, workers: Buffer[ActorRef])
    extends Actor with ActorLogging {
  /** The arguments for all master actors. */
  val hybridArgs = HybridMasterActor.parseArgs(args.list("hybrid"))
  /** The max iteration to run. */
  val maxIter = args.int(MaxIterFlag, hybridArgs.map(_.int(MaxIterFlag)).sum)

  /** The master actor. */
  var masterActor: Option[ActorRef] = None
  /** The start iteration for next master actor. */
  var start = 0
  /** The index of next master actor in the [[hybridArgs]]. */
  var index = 0

  /** Create a master actor from given args. */
  def createMasterActor(args: Args): ActorRef =
    // create master actor with automatic name
    context.actorOf(AkkaUtil.createMasterActorProps(args, db, tester, workers))

  override def receive = {
    // Init indicates DBActor finished initialization before training
    // TrainFinished indicate a master actor finished its training
    // In either situation, we just start the next master actor or finish
    case Init | TrainFinished =>
      // stop the previous master actor, forward through test actor to make
      // sure all test results are presented
      masterActor.foreach(tester ! Forward(_, PoisonPill))
      if (start == maxIter) {
        // previous master already exceeds the iteration limit, just finish
        context.parent ! TrainFinished
      } else {
        // concat the args, hybridArgs(index) will override args for same keys
        val arg = new Args(args.m ++ hybridArgs(index).m - "hybrid")
        // update parameters of each worker
        // go through DB actor to make sure message order
        workers.foreach(db ! Forward(_, UpdateParameter(arg)))
        // iteration to run for this master actor, not exceed iteration limit
        val iter = Math.min(arg.int(MaxIterFlag), maxIter - start)
        log.info("Start {}th master actor for {} iterations", index + 1, iter)
        // create master actor with startIter argument
        masterActor = Some(createMasterActor(arg + (MaxIterFlag -> Seq(iter.toString)) + (StartIterFlag -> List(start.toString))))
        // start training after db actor finished merge and worker updated param
        masterActor.foreach(db ! Forward(_, Init))
        // increase counters
        start = start + iter
        index = (index + 1) % hybridArgs.size
      }
    case Progress => masterActor match {
      // forward to master actor
      case Some(master) => master.tell(ProgressIter(start, maxIter), sender)
      // if master actor is not started yet
      case None => sender ! Progress(0)
    }
    case WorkerCreated(worker) =>
      workers += worker
      context.watch(worker)
      // forward to master actor
      masterActor.foreach(_ ! WorkerCreated(worker))
    case Terminated(worker) if workers.contains(worker) => workers -= worker
  }
}
