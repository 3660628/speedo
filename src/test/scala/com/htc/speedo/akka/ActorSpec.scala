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

import akka.actor.ActorSystem
import akka.testkit.{ TestActorRef, TestKitBase, TestProbe }

import com.twitter.scalding.Args
import com.typesafe.config.ConfigFactory

import org.specs2.AkkaSpecification
import org.specs2.specification.core.Fragments

/**
 * An abstract base class for testing actors
 * @author Wenrui Jiang (roy_jiang@htc.com)
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
abstract class ActorSpec extends AkkaSpecification {
  sequential

  // Disable logging during test
  override implicit lazy val system =
    ActorSystem(getClass.getSimpleName, ConfigFactory.parseString("""
      akka.loglevel = "OFF"
      akka.test.single-expect-default = 2s
    """))

  /** Number of workers. */
  val workerNumber = 3
  /** Number of training iterations. */
  val maxIter = 10
  /** Test Interval. */
  val testInterval = 2
  /** The common command line argument for maxIter and test interval. */
  lazy val commandLine = s"--maxIter $maxIter --test $testInterval"
  /** Test probe for db actor, for testing messages received by db actor. */
  lazy val dbProb = TestProbe()
  /** Test probe for worker actors. */
  lazy val workerProb = (1 to workerNumber).map(_ => TestProbe())
  /** Test probe for test actor. */
  lazy val testProb = TestProbe()

  /** Create master actor for test. The command line is same as real run. */
  def createMasterActor[T <: MasterActor](addtionalArgs: String = ""): T =
    TestActorRef[T](AkkaUtil.createMasterActorProps(
      Args(commandLine + " " + addtionalArgs),
      dbProb.ref, testProb.ref, workerProb.map(_.ref)
    )).underlyingActor

  /** Asserts that a specified message is received using specs matcher */
  implicit class MessageAssertion(val testKit: TestKitBase) {
    def assertMessage(message: Any) = testKit.expectMsg(message) must_== message

    /** @note Blocks for 2 full seconds to make sure no messages are received */
    def assertNoMessage = testKit.expectNoMsg must throwA[Throwable] not
  }

  // shut down system after test
  override def map(fs: => Fragments) = fs ^ step(system.shutdown)
}
