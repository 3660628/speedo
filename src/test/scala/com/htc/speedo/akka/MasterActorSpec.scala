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

import akka.testkit.ImplicitSender

/**
 * Test for basic master actor
 * @author Wenrui Jiang (roy_jiang@htc.com)
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
class MasterActorSpec extends ActorSpec with ImplicitSender {
  "Master Actor" should {
    "parse configuration correctly" in {
      val actor = createMasterActor[MasterActor]()
      actor.count must_== 0
      actor.maxIter must_== maxIter
      actor.testInterval must_== testInterval
      actor.startIter must_== 0
    }
    "handle init state correctly" in {
      val actor = createMasterActor[MasterActor]()
      // should not react to Train message before init is finished
      actor.self.tell(Trained(0f), actor.workers(0))
      dbProb.assertNoMessage
      // simulate init is finished (NOTE: sender is not important)
      actor.self ! Init
      // should start test after init
      testProb.assertMessage(Test)
      // should start train on all workers after init
      workerProb.foreach(_.assertMessage(Train(0)))
      ok
    }
    "handle startIter correctly" in {
      val startIter = 10
      val actor = createMasterActor[MasterActor](s"--startIter $startIter")
      // should not react to Train message before init is finished
      actor.self.tell(Trained(0f), actor.workers(0))
      dbProb.assertNoMessage
      // simulate init is finished (NOTE: sender is not important)
      actor.self ! Init
      // should start test after init
      testProb.assertMessage(Test)
      // should start train on all workers after init
      workerProb.foreach(_.assertMessage(Train(startIter)))
      // progress should not affected by startIter
      actor.self ! Progress
      this.assertMessage(Progress(0f))
    }
    "work correctly as normal asynchronous strategy" in {
      val actor = createMasterActor[MasterActor]()
      // Skip init and change to training state
      actor.context.become(actor.trainState)
      // Simulate training
      (1 to maxIter).foreach { i =>
        // Select a random worker
        val sender = actor.workers(Random.nextInt(workerNumber))
        // Tell master actor that it has finished training
        actor.self.tell(Trained(0f), sender)
        // DB actor should receive a merge message first
        dbProb.assertMessage(Merge(sender))
        // If not enough iterations, trigger next train
        if (i != maxIter) {
          dbProb.assertMessage(Forward(sender, Train(i)))
          // If triggered test correctly
          if (i % 2 == 0) dbProb.assertMessage(Forward(actor.tester, Test))
        } else {
          // If finalizing correctly
          dbProb.assertMessage(Forward(actor.context.parent, TrainFinished))
        }
      }
      ok
    }
    "handle common states correctly" in {
      "handle progress query correctly" in {
        val actor = createMasterActor[MasterActor]()
        actor.self ! Progress
        this.assertMessage(Progress(0f))
        // Change count to a random number
        actor.count = Random.nextInt
        actor.self ! Progress
        this.assertMessage(Progress(actor.count.toFloat / maxIter))
      }
    }
  }
}
