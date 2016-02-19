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
 * Test Weed-Out Master Actor.
 * Simulate workers[0,1,2] finished the interations in such a sequence: 0, 1, 1, 0, 2
 * The drop window 'll be 3, So:
 * after worker 0 finished at iteration 4, it's delta will be keeped
 * after worker 2 finished at iteration 5, it's delta will be droped
 *
 * @author Wenrui Jiang (roy_jiang@htc.com)
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
class WeedOutMasterActorSpec extends ActorSpec {
  override val testInterval = 6

  "Drop Master Actor" should {
    val actor = createMasterActor[WeedOutMasterActor]("--weedout 3")
    val master = actor.self
    var iter = 0
    // Skip init and change to training state
    actor.context.become(actor.trainState)
    val sender_0 = actor.workers(0)
    val sender_1 = actor.workers(1)
    val sender_2 = actor.workers(2)
    "discard deltas for delayed workers" in {
      // worker 0 finished train at iteration 1
      master.tell(Trained(.1f), sender_0)
      iter += 1
      dbProb.assertMessage(Merge(sender_0))
      dbProb.assertMessage(Forward(sender_0, Train(iter)))
      actor.lastUpdates must_== Seq(sender_0)
      actor.count must_== iter

      // worker 1 finished train at iteration 2
      master.tell(Trained(.1f), sender_1)
      iter += 1
      dbProb.assertMessage(Merge(sender_1))
      dbProb.assertMessage(Forward(sender_1, Train(iter)))
      actor.lastUpdates must_== Seq(sender_0, sender_1)
      actor.count must_== iter

      // worker 1 finished train at iteration 3
      master.tell(Trained(.1f), sender_1)
      iter += 1
      dbProb.assertMessage(Merge(sender_1))
      dbProb.assertMessage(Forward(sender_1, Train(iter)))
      actor.lastUpdates must_== Seq(sender_0, sender_1, sender_1)
      actor.count must_== iter

      // worker 0 finished train at iteration 4
      master.tell(Trained(.1f), sender_0)
      iter += 1
      dbProb.assertMessage(Merge(sender_0))
      dbProb.assertMessage(Forward(sender_0, Train(iter)))
      actor.lastUpdates must_== Seq(sender_0, sender_1, sender_1)
      actor.count must_== iter

      // worker 2 finished train at iteration 5
      master.tell(Trained(.1f), sender_2)
      dbProb.assertMessage(ClearWorkerKey(sender_2))
      dbProb.assertMessage(Merge(sender_2, true))
      dbProb.assertMessage(Forward(sender_2, Train(iter)))
      actor.lastUpdates must_== Seq(sender_0, sender_2, sender_1)
      actor.count must_== iter
    }
    "handle worker hot join correctly" in {
      "worker hot join at first few iterations" in {
        "worker hot join when lastUpdates is empty" in {
          actor.updateIndex = 0
          actor.lastUpdates.clear
          actor.maxInterval = workerNumber
          // hot join one worker (should be different with existing workers)
          actor.workerCreated(testActor) // use this test kit as new worker
          // the joined actor should be added as the newest updater
          actor.lastUpdates must_== Seq(testActor)
          actor.updateIndex must_== actor.lastUpdates.size
          actor.maxInterval must_== workerNumber + 1
          // start training on new worker
          this.assertMessage(actor.trainMessage)
        }
        "worker hot join when lastUpdates has 1 updater" in {
          actor.updateIndex = 1
          actor.lastUpdates.clear
          actor.lastUpdates += sender_0
          actor.maxInterval = workerNumber
          // hot join one worker (should be different with existing workers)
          actor.workerCreated(testActor) // use this test kit as new worker
          // the joined actor should be added as the newest updater
          actor.lastUpdates must_== Seq(sender_0, testActor)
          actor.updateIndex must_== actor.lastUpdates.size
          actor.maxInterval must_== workerNumber + 1
          // start training on new worker
          this.assertMessage(actor.trainMessage)
        }
        "worker hot join when lastUpdates has 2 updater" in {
          actor.updateIndex = 2
          actor.lastUpdates.clear
          actor.lastUpdates ++= Seq(sender_0, sender_1)
          actor.maxInterval = workerNumber
          // hot join one worker (should be different with existing workers)
          actor.workerCreated(testActor) // use this test kit as new worker
          // the joined actor should be added as the newest updater
          actor.lastUpdates must_== Seq(sender_0, sender_1, testActor)
          actor.updateIndex must_== actor.lastUpdates.size
          actor.maxInterval must_== workerNumber + 1
          // start training on new worker
          this.assertMessage(actor.trainMessage)
        }
      }
      "worker hot join when updateIndex = 0" in {
        // order of old updaters is 0, 1, 2
        actor.updateIndex = 0
        actor.lastUpdates.clear
        actor.lastUpdates ++= Seq(sender_0, sender_1, sender_2)
        actor.maxInterval = actor.lastUpdates.size
        // hot join one worker (should be different with existing workers)
        actor.workerCreated(testActor) // use this test kit as new worker
        // the joined actor should be added as the newest updater
        actor.lastUpdates must_== Seq(testActor, sender_0, sender_1, sender_2)
        // oldest updaters are not affacted
        actor.lastUpdates(actor.updateIndex) must_== sender_0
        actor.maxInterval must_== actor.lastUpdates.size
        // start training on new worker
        this.assertMessage(actor.trainMessage)
      }
      "worker hot join when updateIndex = 1" in {
        // order of old updaters is 1, 2, 0
        actor.updateIndex = 1
        actor.lastUpdates.clear
        actor.lastUpdates ++= Seq(sender_0, sender_1, sender_2)
        actor.maxInterval = actor.lastUpdates.size
        // hot join one worker (should be different with existing workers)
        actor.workerCreated(testActor) // use this test kit as new worker
        // the joined actor should be added as the newest updater
        actor.lastUpdates must_== Seq(sender_0, testActor, sender_1, sender_2)
        // oldest updaters are not affacted
        actor.lastUpdates(actor.updateIndex) must_== sender_1
        actor.maxInterval must_== actor.lastUpdates.size
        // start training on new worker
        this.assertMessage(actor.trainMessage)
      }
      "worker hot join when updateIndex = 2" in {
        // order of old updaters is 2, 0, 1
        actor.updateIndex = 2
        actor.lastUpdates.clear
        actor.lastUpdates ++= Seq(sender_0, sender_1, sender_2)
        actor.maxInterval = actor.lastUpdates.size
        // hot join one worker (should be different with existing workers)
        actor.workerCreated(testActor) // use this test kit as new worker
        // the joined actor should be added as the newest updater
        actor.lastUpdates must_== Seq(sender_0, sender_1, testActor, sender_2)
        // oldest updaters are not affacted
        actor.lastUpdates(actor.updateIndex) must_== sender_2
        actor.maxInterval must_== actor.lastUpdates.size
        // start training on new worker
        this.assertMessage(actor.trainMessage)
      }
    }
    "handle terminated worker correctly" in {
      "worker terminate at first few iterations" in {
        "worker terminate when lastUpdates is empty" in {
          actor.updateIndex = 0
          actor.lastUpdates.clear
          actor.maxInterval = workerNumber
          // terminate a worker (which one is not important)
          actor.workerTerminated(sender_0)
          // oldest updater must be removed and second oldest becomes oldest
          actor.lastUpdates must beEmpty
          actor.updateIndex must_== actor.lastUpdates.size
          actor.maxInterval must_== workerNumber - 1
        }
        "worker terminate when lastUpdates has 1 updater" in {
          actor.updateIndex = 1
          actor.lastUpdates.clear
          actor.lastUpdates += sender_0
          actor.maxInterval = workerNumber
          // terminate a worker (which one is not important)
          actor.workerTerminated(sender_0)
          // oldest updater must be removed and second oldest becomes oldest
          actor.lastUpdates must_== Seq(sender_0)
          actor.updateIndex must_== actor.lastUpdates.size
          actor.maxInterval must_== workerNumber - 1
        }
        "worker terminate when lastUpdates has 2 updater" in {
          actor.updateIndex = 2
          actor.lastUpdates.clear
          actor.lastUpdates ++= Seq(sender_0, sender_1)
          actor.maxInterval = workerNumber
          // terminate a worker (which one is not important)
          actor.workerTerminated(sender_0)
          // the joined actor should be added as the newest updater
          actor.lastUpdates must_== Seq(sender_0, sender_1)
          // not first few iterations any more
          actor.updateIndex must_== 0
          actor.maxInterval must_== workerNumber - 1
        }
      }
      "worker terminate when updateIndex = 0" in {
        // order of old updaters is 0, 1, 2
        actor.updateIndex = 0
        actor.lastUpdates.clear
        actor.lastUpdates ++= Seq(sender_0, sender_1, sender_2)
        actor.maxInterval = actor.lastUpdates.size
        // terminate a worker (which one is not important)
        actor.workerTerminated(sender_0)
        // oldest updater must be removed and second oldest becomes oldest
        actor.lastUpdates must_== Seq(sender_1, sender_2)
        actor.lastUpdates(actor.updateIndex) must_== sender_1
        actor.maxInterval must_== actor.lastUpdates.size
      }
      "worker terminate when updateIndex = 1" in {
        // order of old updaters is 1, 2, 0
        actor.updateIndex = 1
        actor.lastUpdates.clear
        actor.lastUpdates ++= Seq(sender_0, sender_1, sender_2)
        actor.maxInterval = actor.lastUpdates.size
        // terminate a worker (which one is not important)
        actor.workerTerminated(sender_0)
        // oldest updater must be removed and second oldest becomes oldest
        actor.lastUpdates must_== Seq(sender_0, sender_2)
        actor.lastUpdates(actor.updateIndex) must_== sender_2
        actor.maxInterval must_== actor.lastUpdates.size
      }
      "worker terminate when updateIndex = 2" in {
        // order of old updaters is 2, 0, 1
        actor.updateIndex = 2
        actor.lastUpdates.clear
        actor.lastUpdates ++= Seq(sender_0, sender_1, sender_2)
        actor.maxInterval = actor.lastUpdates.size
        // terminate a worker (which one is not important)
        actor.workerTerminated(sender_0)
        // oldest updater must be removed and second oldest becomes oldest
        actor.lastUpdates must_== Seq(sender_0, sender_1)
        actor.lastUpdates(actor.updateIndex) must_== sender_0
        actor.maxInterval must_== actor.lastUpdates.size
      }
    }
  }
}
