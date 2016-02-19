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

import scala.util.Random

/**
 * Test Synchronous Master Actor
 * @author Wenrui Jiang (roy_jiang@htc.com)
 */
class SynchronousMasterActorSpec extends ActorSpec {
  override val maxIter = 10 // runs long enough
  override val testInterval = 20 // no tests triggered

  "Synchronous Master Actor" should {
    val actor = createMasterActor[SynchronousMasterActor]("--sync")
    val master = actor.self
    // Skip init and change to training state
    actor.context.become(actor.trainState)
    val sender_0 = actor.workers(0)
    val sender_1 = actor.workers(1)
    val sender_2 = actor.workers(2)
    "work correctly for synchronous strategy" in {
      val sender = actor.workers(Random.nextInt(workerNumber))
      actor.waitingSet must containTheSameElementsAs(actor.workers)
      // NOT all workers finished training, the master will be waiting status.
      master.tell(Trained(.1f), sender)
      actor.waitingSet must containTheSameElementsAs(actor.workers - sender)
      dbProb.expectNoMsg
      workerProb.foreach(_.expectNoMsg)
      // All workers finished the training, the master will merge all deltas.
      (actor.workers - sender).foreach(master.tell(Trained(.1f), _))
      dbProb.assertMessage(UpdateWorkers(actor.workers.map(_.path.name).toSet))
      dbProb.assertMessage(MergeAll)
      actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }
      actor.waitingSet must containTheSameElementsAs(actor.workers)
    }
    "handle worker hot join correctly" in {
      // hot join one worker (should be different with existing workers)
      master ! WorkerCreated(testActor) // use this test kit as new worker
      actor.workers must_== Seq(sender_0, sender_1, sender_2, testActor)
      // nothing should happen
      dbProb.expectNoMsg

      // finish one iteration
      (actor.workers - testActor).foreach(master.tell(Trained(.1f), _))
      dbProb.assertMessage(MergeAll)
      // all workers including new one should start training new iteration
      actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }

      // finish another iteration, now new worker should be used
      actor.workers.foreach(master.tell(Trained(.1f), _))
      dbProb.assertMessage(UpdateWorkers(actor.workers.map(_.path.name).toSet))
      dbProb.assertMessage(MergeAll)
      actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }

      // clean up
      actor.workers -= testActor
      actor.lastUpdateWorkers = Set(actor.workers: _*)
      actor.waitingSet -= testActor
      ok
    }
    "handle terminated worker correctly" in {
      "worker terminate before train finished" in {
        "worker is the last worker in waiting list" in {
          // worker 0 finished
          master.tell(Trained(.1f), sender_0)
          // worker 1 finished
          master.tell(Trained(.1f), sender_1)

          // simulate worker 2 is terminated
          actor.workers -= sender_2
          actor.workerTerminated(sender_2)
          dbProb.assertMessage(UpdateWorkers(actor.workers.map(_.path.name).toSet))
          dbProb.assertMessage(MergeAll)
          actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }
          // stopped worker should not start training
          workerProb(2).msgAvailable must beFalse

          // clean up
          actor.workers += sender_2
          actor.lastUpdateWorkers = Set(actor.workers: _*)
          actor.waitingSet += sender_2
          ok
        }
        "worker is not the last worker in waiting list" in {
          // worker 0 finished
          master.tell(Trained(.1f), sender_0)

          // simulate worker 2 is terminated
          actor.workers -= sender_2
          actor.workerTerminated(sender_2)
          // nothing should happened
          dbProb.expectNoMsg

          // worker 1 finished
          master.tell(Trained(.1f), sender_1)
          dbProb.assertMessage(UpdateWorkers(actor.workers.map(_.path.name).toSet))
          dbProb.assertMessage(MergeAll)
          actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }
          // stopped worker should not start training
          workerProb(2).msgAvailable must beFalse

          // clean up
          actor.workers += sender_2
          actor.lastUpdateWorkers = Set(actor.workers: _*)
          actor.waitingSet += sender_2
          ok
        }
      }
      "worker terminate after train finished" in {
        // worker 2 finished
        master.tell(Trained(.1f), sender_2)
        // worker 1 finished
        master.tell(Trained(.1f), sender_1)
        actor.waitingSet must_== Set(sender_0)
        actor.mergeSet must containTheSameElementsAs(Seq(sender_1, sender_2))

        // simulate worker 2 is terminated
        actor.workers -= sender_2
        actor.workerTerminated(sender_2)
        // waiting set and merge set is not affacted
        actor.waitingSet must_== Set(sender_0)
        actor.mergeSet must containTheSameElementsAs(Seq(sender_1, sender_2))

        // worker 0 finished
        master.tell(Trained(.1f), sender_0)
        // show merge worker 0, 1, 2 at this iteration
        dbProb.assertMessage(MergeAll)
        // all workers including new one should start training new iteration
        actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }
        // stopped worker should not start training
        workerProb(2).msgAvailable must beFalse

        // worker 0 and 1 finished 2nd iteration
        master.tell(Trained(.1f), sender_0)
        master.tell(Trained(.1f), sender_1)
        // now should update merge list to db actor
        dbProb.assertMessage(UpdateWorkers(actor.workers.map(_.path.name).toSet))
        dbProb.assertMessage(MergeAll)
        actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }

        // clean up
        actor.workers += sender_2
        actor.lastUpdateWorkers = Set(actor.workers: _*)
        actor.waitingSet += sender_2
        ok
      }
    }
  }
}
