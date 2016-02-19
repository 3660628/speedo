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

import com.twitter.algebird.Semigroup
import com.twitter.scalding.Args
import com.twitter.storehaus.FutureOps
import com.twitter.util.Await

import com.htc.speedo.caffe.{ CaffeWorker, NetParameterOperation }

import CaffeWorker._

/**
 * A database actor stays in the master machine to hold a copy of weights in memory, merges
 * snapshots from workers and forward messages (Train or Test) to workers.
 *
 * Parameters:
 *  - `--factor <factor>`: (Optional) If provided, divide the delta trained of each worker by the
 * given factor. Only works if non of the parameters in [[ParameterActor]] is defined.
 *
 * @note This actor should be in the same jvm as the master actor, so forwarding messages should not
 * be an overhead.
 * @see [[ParameterActor]] for other arguments required by the actor.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 * @author Wenrui Jiang (roy_jiang@htc.com)
 */
case class DBActor(args: Args) extends ParameterActor(args) {
  /** working directory for caffe worker */
  val workingDir = "caffe.db"
  /** The worker for this actor. Used for init and merge delta. */
  val caffeWorker = CaffeWorker(args + ("baseDir" -> Seq(workingDir)))
  /** The name of the key in store. */
  val name = caffeWorker.getKey
  /** Create the same store as in [[CaffeWorker]] */
  val store = caffeWorker.snapshotStore
  /** The same semigroup as in [[CaffeWorker]], but for Option[Array[Byte]]. */
  val semigroup = Semigroup.optionSemigroup(store.semigroup)
  /**
   * define store keys for all workers for synchronous master actor, empty by default.
   * Synchronous master updates it before first iteration is merged.
   */
  var workerKeys = Set[String]()
  /** If provided, divide the delta by the factor. */
  val factor = args.optional("factor").map(_.toFloat)
  /** The snapshot cached in memory for all situations. */
  var snapshot: Option[Array[Byte]] = None

  override def receive = {
    // Init weights
    // TODO: support to reuse previous history
    case Init(resume) =>
      // init weights and save to local copy (same as storehaus)
      snapshot = Some(caffeWorker.init(resume))
      // tell the master that initialization is finished
      sender ! Init
    // deletes the key of worker's suffix in storehaus
    case ClearWorkerKey(worker) =>
      Await.result(store.put((name + worker.path.name) -> None))
    case UpdateParameter(arg) =>
      // update training parameters in base class
      updateParameter(arg)
      // update solver parameters in caffe worker
      caffeWorker.updateParameter(arg)
    // Forward test message if running asynchronously
    case Forward(worker, Test) if !synchronous && weightUpdate != GradientOnly =>
      // put snapshot to global key first since snapshot is written to worker's
      // suffix key when handling Merge message
      Await.result(store.put(name, snapshot))
      worker.tell(Test, sender)
    // Forward message
    case Forward(worker, message) => worker.tell(message, sender)
    // Merge snapshot of the worker, only meaningful for asynchronous master
    case Merge(worker, silent) if !synchronous =>
      Await.result(store.get(name + worker.path.name)) match {
        case Some(delta) => (weightUpdate, movingRate) match {
          // each worker updates the weight in their own pace
          case (SelfPaceFullUpdate, Some(rate)) =>
            // movingRate * (local - global)
            val elasticDiff = NetParameterOperation.multiply(
              NetParameterOperation.minus(delta, snapshot.get), rate
            )
            // update local weight
            val weight = NetParameterOperation.minus(delta, elasticDiff)
            // update global weight
            snapshot = semigroup.plus(snapshot, Some(elasticDiff))
            // put to worker's suffix key (worker will read it in next train)
            Await.result(store.put(name + worker.path.name, Some(weight)))
          // TODO: save snapshot to global key (not only before Test)
          // This should not happen, but just in case something is going wrong
          case (SelfPaceFullUpdate, None) =>
            log.error("Can't merge self pace update as moving rate is not set")
          // Add the deltas to weights and put to store
          case (FullUpdate, _) =>
            snapshot = semigroup.plus(snapshot, factor.map(f =>
              NetParameterOperation.divide(delta, f)).orElse(Some(delta)))
            // put to worker's suffix key (worker will read it in next train)
            Await.result(store.put(name + worker.path.name, snapshot))
          // TODO: save snapshot to global key (not only before Test)
          // Merge deltas and write to suffix key
          case (GradientOnly, _) =>
            snapshot = Some(caffeWorker.mergeDelta(delta, worker.path.name))
        }
        case None => // this may happen in drop master actor
          if (!silent) log.warning("Delta from {} is empty!", worker.path.name)
          // put snapshot to worker's suffix key (will read it in next train)
          Await.result(store.put(name + worker.path.name, snapshot))
      }
    // update keys of active workers
    case UpdateWorkers(keys) => workerKeys = keys.map(name + _)
    // Merge snapshot of all workers, only meaningful for synchronous master
    case MergeAll if synchronous =>
      // Get deltas for all workers
      val futures = store.multiGet(workerKeys)
      val deltas = Await.result(FutureOps.mapCollect(futures)).values
      // TODO: Shall we fail here?
      if (deltas.exists(_.isEmpty)) log.error("Delta contains empty!")
      // average of all deltas, ignores None, but always divide by #workers
      semigroup.sumOption(deltas).flatten.map(NetParameterOperation.divide(_, workerKeys.size)) match {
        case Some(averaged) => weightUpdate match {
          // Add the deltas to weights and put to store
          case FullUpdate =>
            snapshot = semigroup.plus(snapshot, Some(averaged))
            Await.result(store.put(name, snapshot))
          // Merge deltas and write to global key
          case GradientOnly => snapshot = Some(caffeWorker.mergeDelta(averaged))
          // This should not happen, but just in case something is going wrong
          case SelfPaceFullUpdate =>
            log.error("Does not support self pace update in synchronous master")
        }
        case None => log.error("All deltas are empty!")
      }
  }

  override def postStop = caffeWorker.close
}
