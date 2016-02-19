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

import java.util.UUID

import akka.actor.{ ActorRef, PoisonPill, Props }
import akka.testkit.{ ImplicitSender, TestActorRef, TestProbe }

import com.twitter.scalding.Args

object HybridMasterActorSpec {
  /** Actor creation must be put in companion object to pass serialization. */
  def createHybridMasterActorProps(args: Args, db: ActorRef, tester: ActorRef,
    workers: Seq[ActorRef], master1: ActorRef, master2: ActorRef): Props =
    Props(new HybridMasterActor(args, db, tester, workers.toBuffer) {
      override def createMasterActor(args: Args) = if (args.boolean("sync")) master1 else master2
    })
}

/**
 * Test for hybrid master actor
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
class HybridMasterActorSpec extends ActorSpec with ImplicitSender {
  // two fake master actors
  lazy val masterProb1 = TestProbe()
  lazy val masterProb2 = TestProbe()

  /** argument for creating [[HybridMasterActor]] in test */
  val hybridArgs = "--hybrid =sync =maxIter 2 = =maxIter 1"
  /** The argument for first master actor. */
  val masterArgs1 = "--sync --maxIter 2"
  /** The argument for second master actor. */
  val masterArgs2 = "--maxIter 1"

  // remove maxIter
  override lazy val commandLine = s"--test $testInterval"

  /** Create master actor for test. The command line is same as real run. */
  def createHybridMasterActor(addtionalArgs: String = ""): HybridMasterActor = {
    val props = HybridMasterActorSpec.createHybridMasterActorProps(
      Args(commandLine + " " + addtionalArgs),
      dbProb.ref, testProb.ref, workerProb.map(_.ref), masterProb1.ref, masterProb2.ref
    )
    TestActorRef(props, testActor, UUID.randomUUID.toString).underlyingActor
  }

  /** checks the messages received by db actor when a master actor started */
  def checkStartMaster(master: TestProbe, args: String): Unit = {
    // update parameters for each worker
    workerProb.foreach(worker => dbProb.assertMessage(
      Forward(worker.ref, UpdateParameter(Args(commandLine + " " + args)))
    ))
    // tell master actor everything is ready
    dbProb.assertMessage(Forward(master.ref, Init))
  }

  "Hybrid Master Actor" should {
    "parse --hybrid argument correctly" in {
      val arg1 = List("=key1", "value1=", "value=1")
      val arg2 = List("=key2", "value2", "value3")
      (1 to 10).foreach { i =>
        val separator = List(Seq.fill(i)("=").mkString(""))
        val args = HybridMasterActor.parseArgs(
          separator ::: arg1 ::: separator ::: arg2 ::: separator
        )
        args.size must_== 4
        args(0).toString must beEmpty
        args(1).toString must_== "--key1 value1= value=1"
        args(2).toString must_== "--key2 value2 value3"
        args(3).toString must beEmpty
      }
      ok
    }
    "create master actors correctly" in {
      "if maxIter is not given" in {
        val master = createHybridMasterActor(hybridArgs)
        master.start must_== 0
        // simulate DB actor finished init
        master.self ! Init
        // check messages to start master
        checkStartMaster(masterProb1, masterArgs1)
        master.start must_== 2
        // simualte first master finished
        master.self ! TrainFinished
        testProb.assertMessage(Forward(masterProb1.ref, PoisonPill))
        // check messages to start master
        checkStartMaster(masterProb2, masterArgs2)
        master.start must_== master.maxIter
        // simualte second master finished
        master.self ! TrainFinished
        testProb.assertMessage(Forward(masterProb2.ref, PoisonPill))
        // hybrid master actor should have finished train
        this.assertMessage(TrainFinished)
      }
      "if maxIter is smaller than sum of masters' maxIter" in {
        val master = createHybridMasterActor("--maxIter 1 " + hybridArgs)
        master.start must_== 0
        // simulate DB actor finished init
        master.self ! Init
        // check messages to start master
        checkStartMaster(masterProb1, masterArgs1)
        master.start must_== master.maxIter
        // simualte first master finished
        master.self ! TrainFinished
        testProb.assertMessage(Forward(masterProb1.ref, PoisonPill))
        // hybrid master actor should have finished train
        this.assertMessage(TrainFinished)
      }
      "if maxIter is larger than sum of master's maxIter" in {
        val master = createHybridMasterActor("--maxIter 4 " + hybridArgs)
        master.start must_== 0
        // simulate DB actor finished init
        master.self ! Init
        // check messages to start master
        checkStartMaster(masterProb1, masterArgs1)
        master.start must_== 2
        // simualte first master finished
        master.self ! TrainFinished
        testProb.assertMessage(Forward(masterProb1.ref, PoisonPill))
        // check messages to start master
        checkStartMaster(masterProb2, masterArgs2)
        master.start must_== 3
        // simualte second master finished
        master.self ! TrainFinished
        testProb.assertMessage(Forward(masterProb2.ref, PoisonPill))
        // check messages to start master
        checkStartMaster(masterProb1, masterArgs1)
        master.start must_== master.maxIter
        // simualte first master finished again
        master.self ! TrainFinished
        testProb.assertMessage(Forward(masterProb1.ref, PoisonPill))
        // hybrid master actor should have finished train
        this.assertMessage(TrainFinished)
      }
    }
    "handle Progress message correctly" in {
      val master = createHybridMasterActor(hybridArgs)
      // before master started, progress is 0
      master.self ! Progress
      this.assertMessage(Progress(0f))
      master.self ! Init
      // check messages to start master
      checkStartMaster(masterProb1, masterArgs1)
      // after master started, forward to master
      master.self ! Progress
      masterProb1.assertMessage(ProgressIter(2, 3))
    }
  }
}
