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

/**
 * Test Partial Synchronous Master Actor.
 * Simulate workers[0,1,2] finished the interations in such a sequence: 0, 0, 1, 2
 * Status after each worker finishing the iteration:
 * worker 0: normal
 * worker 0: normal => catchup
 * worker 1: catchup
 * worker 2: catchup => normal
 *
 * @author Wenrui Jiang (roy_jiang@htc.com)
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
class PSMasterActorSpec extends ActorSpec {
  override val maxIter = 30 // runs long enough
  override val testInterval = 50 // no tests triggered

  "Partial Synchronous Master Actor" should {
    val actor = createMasterActor[PSMasterActor]("--maxAdvance 2")
    val master = actor.self
    var iter = 0
    // Skip init and change to training state
    actor.context.become(actor.trainState)
    val sender_0 = actor.workers(0)
    val sender_1 = actor.workers(1)
    val sender_2 = actor.workers(2)
    "work correctly for normal and catch up strategy" in {
      // worker 0 finished train for iteration 0
      master.tell(Trained(.1f), sender_0)
      iter += 1
      dbProb.assertMessage(Merge(sender_0))
      dbProb.assertMessage(Forward(sender_0, Train(iter)))
      actor.catchup must_== false
      actor.catchupWorkers must beEmpty

      // worker 0 finished train for iteration 1
      master.tell(Trained(.1f), sender_0)
      iter += 1
      dbProb.assertMessage(Merge(sender_0))
      workerProb(0).expectNoMsg
      actor.catchup must_== true
      actor.catchupWorkers must containTheSameElementsAs(actor.workers.toBuffer - sender_0)

      // worker 1 finished train for iteration 2
      master.tell(Trained(.1f), sender_1)
      iter += 1
      dbProb.assertMessage(Merge(sender_1))
      workerProb(1).expectNoMsg
      actor.catchup must_== true
      actor.catchupWorkers must containTheSameElementsAs(Seq(sender_2))

      // worker 2 finished train for iteration 3
      master.tell(Trained(.1f), sender_2)
      iter += 1
      dbProb.assertMessage(Merge(sender_2))
      actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, Train(iter))) }
      actor.catchup must_== false
      actor.catchupWorkers must beEmpty
    }
    "handle worker hot join correctly" in {
      "worker hot join when catch up" in {
        // worker 0 finished train for iteration 0
        master.tell(Trained(.1f), sender_0)
        dbProb.assertMessage(Merge(sender_0))
        dbProb.assertMessage(Forward(sender_0, actor.trainMessage))
        // worker 0 finished train for iteration 1 and it goes to catch up
        master.tell(Trained(.1f), sender_0)
        dbProb.assertMessage(Merge(sender_0))
        actor.catchup must_== true
        val catchupWorkers = actor.workers.toBuffer - sender_0
        actor.catchupWorkers must containTheSameElementsAs(catchupWorkers)

        // hot join one worker (should be different with existing workers)
        master ! WorkerCreated(testActor) // use this test kit as new worker
        // catchup workers are not changed
        actor.catchupWorkers must containTheSameElementsAs(catchupWorkers)
        // total workers contains newly added workers
        actor.workers must containTheSameElementsAs(workerProb.map(_.ref) :+ testActor)

        // worker 1 finished train for iteration 2
        master.tell(Trained(.1f), sender_1)
        dbProb.assertMessage(Merge(sender_1))
        // worker 2 finished train for iteration 3 and back to normal
        master.tell(Trained(.1f), sender_2)
        dbProb.assertMessage(Merge(sender_2))
        actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }
        actor.catchup must_== false
        actor.catchupWorkers must beEmpty

        // clean up
        actor.workers -= testActor
        actor.workerIters -= testActor
        actor.context.unwatch(testActor)
        ok
      }
      "worker hot join when not catch up" in {
        // just check we have all worker iters set to 1
        actor.workerIters.toSeq must containTheSameElementsAs(actor.workers.map(_ -> 1).toSeq)
        // hot join one worker (should be different with existing workers)
        actor.workerCreated(testActor) // use this test kit as new worker
        this.assertMessage(actor.trainMessage)
        actor.workerIters(testActor) must_== 1

        // clean up
        actor.workerIters -= testActor
        ok
      }
    }
    "handle terminated worker correctly" in {
      "worker terminate when catch up" in {
        "worker terminate before train finished" in {
          // worker 0 finished train for iteration 0
          master.tell(Trained(.1f), sender_0)
          dbProb.assertMessage(Merge(sender_0))
          dbProb.assertMessage(Forward(sender_0, actor.trainMessage))
          // worker 0 finished train for iteration 1 and it goes to catch up
          master.tell(Trained(.1f), sender_0)
          dbProb.assertMessage(Merge(sender_0))
          actor.catchup must_== true
          val catchupWorkers = actor.workers.toBuffer - sender_0
          actor.catchupWorkers must containTheSameElementsAs(catchupWorkers)

          // simulate worker0 is terminated
          actor.workers -= sender_0
          actor.workerTerminated(sender_0)
          actor.workerIters.get(sender_0) must beNone
          // catchup workers are not changed
          actor.catchupWorkers must containTheSameElementsAs(catchupWorkers)
          // worker0 is removed from total workers
          actor.workers must containTheSameElementsAs(Seq(sender_1, sender_2))

          // worker 1 finished train for iteration 2
          master.tell(Trained(.1f), sender_1)
          dbProb.assertMessage(Merge(sender_1))
          // worker 2 finished train for iteration 3 and back to normal
          master.tell(Trained(.1f), sender_2)
          dbProb.assertMessage(Merge(sender_2))
          actor.workers.foreach { sender => dbProb.assertMessage(Forward(sender, actor.trainMessage)) }
          actor.catchup must_== false
          actor.catchupWorkers must beEmpty

          // clean up
          actor.workerIters += sender_0 -> 1
          sender_0 +=: actor.workers // prepend worker0 back to total workers
          ok
        }
        "worker terminate after train finished" in {
          // worker 0 finished train for iteration 0
          master.tell(Trained(.1f), sender_0)
          dbProb.assertMessage(Merge(sender_0))
          dbProb.assertMessage(Forward(sender_0, actor.trainMessage))
          // worker 0 finished train for iteration 1 and it goes to catch up
          master.tell(Trained(.1f), sender_0)
          dbProb.assertMessage(Merge(sender_0))
          actor.catchup must_== true
          val catchupWorkers = actor.workers.toBuffer - sender_0
          actor.catchupWorkers must containTheSameElementsAs(catchupWorkers)

          // simulate worker2 is terminated
          actor.workers -= sender_2
          actor.workerTerminated(sender_2)
          actor.workerIters.get(sender_2) must beNone
          // worker2 should be removed from catchup workers
          actor.catchupWorkers must containTheSameElementsAs(Seq(sender_1))
          // worker2 is removed from total workers
          actor.workers must containTheSameElementsAs(Seq(sender_0, sender_1))
          // we are still catching up (waiting for worker1)
          actor.catchup must_== true

          // simulate worker1 is terminated
          actor.workers -= sender_1
          actor.workerTerminated(sender_1)
          actor.workerIters.get(sender_1) must beNone
          // worker1 is removed from total workers
          actor.workers must_== Seq(sender_0)
          // we should now back to normal
          actor.catchup must_== false
          actor.catchupWorkers must beEmpty
          dbProb.assertMessage(Forward(sender_0, actor.trainMessage))

          // clean up
          actor.workerIters ++= Seq(sender_1 -> 1, sender_2 -> 1)
          actor.workers ++= Seq(sender_1, sender_2)
          ok
        }
      }
      "worker terminate when not catch up" in {
        // terminate a worker
        actor.workerTerminated(sender_0)
        actor.workerIters.get(sender_0) must beNone

        // clean up
        actor.workerIters += sender_0 -> 1
        ok
      }
    }
  }
}
