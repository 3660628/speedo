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

import akka.actor.{ ActorIdentity, Address, PoisonPill }
import akka.testkit.{ ImplicitSender, TestActorRef, TestProbe }

import com.twitter.scalding.Args

import HostMasterActor.IdentifyWorker

/**
 * Test for host master actor
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
class HostMasterActorSpec extends ActorSpec with ImplicitSender {
  /** Test probe for master actor. */
  lazy val masterProb = TestProbe()

  def createHostMasterActor(worker: Int = workerNumber, resume: Boolean = false): HostMasterActor = {
    val args = Args(s"--worker $worker" + (if (resume) " --resume" else ""))
    TestActorRef(new HostMasterActor(args) {
      override def createMasterActor = masterProb.ref
      override def createDbActor = dbProb.ref
      override def createWorker(index: Int, address: Address) = workerProb(index).ref
      override def createTester = testProb.ref

      // Do not stop this akka system, but do stop host slave actors
      def stopAkka: Receive = {
        case StopAkka => hostActors.foreach(_ ! StopAkka)
        // override messages that chagnes actor's behavior
        case message @ ActorIdentity(IdentifyWorker(_), _) =>
          // handle the message as usual
          initOrTrainingState(message)
          // if master is created, actor's behavior has changed.
          // We need to make sure StopAkka message is handled as we wanted.
          if (masterActor.isDefined)
            context.become(stopAkka orElse commonState orElse initOrTrainingState orElse trainingState)
      }
      override def receive: Receive = stopAkka orElse super.receive
    }).underlyingActor
  }

  "Host Master Actor" should {
    "parse configuration correctly" in {
      val actor = createHostMasterActor()
      actor.numWorkers must_== workerNumber
      actor.hostActors must beEmpty
      actor.workerActors must beEmpty
      actor.masterActor must beNone
      actor.dbActor must_== dbProb.ref
      actor.testActor must_== testProb.ref
      // stop the actor, so it does not affact later tests
      actor.self ! PoisonPill
      ok
    }
    "handle start up correctly" in {
      val actor = createHostMasterActor()
      "handle worker join when not enough workers" in {
        (1 to (workerNumber - 1)).foreach { i =>
          // simulate an akka system joins, the address is not used in test
          actor.self ! Join
          // Host actors should contain i senders (i.e. ref)
          actor.hostActors must containTheSameElementsAs(Seq.fill(i)(testActor))
          // workers created
          actor.workerActors must containTheSameElementsAs(workerProb.take(i).map(_.ref))
          // master actor is not created
          actor.masterActor must beNone
          dbProb.assertMessage(ClearWorkerKey(workerProb(i - 1).ref))
        }
        ok
      }
      "handle last worker join correctly" in {
        // the last worker joins
        actor.self ! Join
        actor.hostActors must containTheSameElementsAs(Seq.fill(workerNumber)(testActor))
        actor.workerActors must containTheSameElementsAs(workerProb.map(_.ref))
        actor.masterActor must beSome(masterProb.ref)
        dbProb.assertMessage(ClearWorkerKey(workerProb.last.ref))
        dbProb.assertMessage(Init(false))
      }
      "send init message correctly when --resume is given" in {
        val actor = createHostMasterActor(1, true).self
        actor ! Join
        dbProb.assertMessage(Init(true))
        actor ! PoisonPill
        ok
      }
      step { actor.self ! PoisonPill }
    }
    "handle progress query correctly" in {
      val actor = createHostMasterActor(1, true).self
      "when master actor is not started" in {
        actor ! Progress
        // return 0 progress
        this.assertMessage(Progress(0))
      }
      "when master actor is started" in {
        // join one worker to start master
        actor ! Join
        // consume the message in db, so following tests are not affacted
        dbProb.assertMessage(Init(true))
        actor ! Progress
        // forward Progress message to master prob
        masterProb.assertMessage(Progress)
        masterProb.lastSender must_== testActor
      }
      step { actor ! PoisonPill }
    }
    // Following three tests will terminate actors in probs, so must run last
    "handle exceptions correctly" in {
      val workerNumber = 2
      val actor = createHostMasterActor(workerNumber, true)
      // join two workers to start master
      actor.self ! Join
      actor.self ! Join
      // consume the message in db, so following tests are not affacted
      dbProb.assertMessage(Init(true))
      "handle failure to create worker correctly" in {
        // if the Identify of the created worker failed
        actor.self ! ActorIdentity(IdentifyWorker(actor.self), None)
        // stop the whole system (i.e. send messages to all host slaves)
        (1 to workerNumber).foreach(_ => this.assertMessage(StopAkka))
        this.msgAvailable must beFalse
        // if the Identify returned different actor (should NOT happen)
        actor.self ! ActorIdentity(IdentifyWorker(actor.self), Some(masterProb.ref))
        // stop the whole system (i.e. send messages to all host slaves)
        (1 to workerNumber).foreach(_ => this.assertMessage(StopAkka))
        this.msgAvailable must beFalse
      }
      "handle termination of master actor correctly" in {
        masterProb.ref ! PoisonPill
        // stop the whole system (i.e. send messages to all host slaves)
        (1 to workerNumber).foreach(_ => this.assertMessage(StopAkka))
        this.msgAvailable must beFalse
      }
      "handle termination of db actor correctly" in {
        dbProb.ref ! PoisonPill
        // stop the whole system (i.e. send messages to all host slaves)
        (1 to workerNumber).foreach(_ => this.assertMessage(StopAkka))
        this.msgAvailable must beFalse
      }
      "handle termination of worker actor correctly" in {
        // stop the first worker
        workerProb(0).ref ! PoisonPill
        // remove the stopped worker from list (only one worker reamining now)
        actor.workerActors must containTheSameElementsAs(Seq(workerProb(1).ref))
        // should not stop whole system
        this.msgAvailable must beFalse

        // stop the second (last) worker
        workerProb(1).ref ! PoisonPill
        // worker list should be empty
        actor.workerActors must beEmpty
        this.msgAvailable must beFalse
      }
      "handle termination of test actor correctly" in {
        // stop actor before train is finished
        testProb.ref ! PoisonPill
        // nothing should happen
        this.expectNoMsg
        // simulate the train is finished
        actor.self ! TrainFinished
        // stop the whole system (i.e. send messages to all host slaves)
        (1 to workerNumber).foreach(_ => this.assertMessage(StopAkka))
        this.msgAvailable must beFalse
      }
    }
  }
}
