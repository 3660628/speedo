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

import scala.util.Try

import com.twitter.scalding.Args

import com.htc.speedo.caffe.CaffeWorker

/**
 * Creating a worker actor that invokes [[CaffeWorker]]. The arguments of this actor are passed to
 * [[CaffeWorker]].
 *
 * Parameters:
 *  - `--test <test interval>`: (optional) If set to 0, then tests are skipped.
 * Default is non-zero.
 * @see [[ParameterActor]] for arguments required by the actor itself.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class WorkerActor(args: Args) extends ParameterActor(args) {
  /** If we run test or not. */
  val runTest = args.int("test", 1) > 0

  // The function to close the lazy worker.
  // If worker is not created, we don't need to close it
  private var closeWorker: Option[() => Unit] = None

  /**
   * The worker of this actor.
   * Marked as lazy so no worker is created if skip tests.
   */
  lazy val worker = {
    val worker = CaffeWorker(new Args(args.m - "snapshot"))
    closeWorker = Some(() => worker.close)
    worker
  }

  // Don't be lazy if worker is used fro training
  if (args.boolean("suffix")) worker

  override def receive = {
    case UpdateParameter(arg) =>
      // update training parameters in base class
      updateParameter(arg)
      // update solver parameters in caffe worker
      worker.updateParameter(arg)
    case Train(iteration) =>
      log.info("{} is training the {}th iteration.", self.path.name, iteration)
      worker.setIteration(iteration)
      // use training parameters from base class
      sender ! Trained(worker.train(!synchronous, weightUpdate))
    case Test if runTest =>
      // handle error for test actor, so no fault tolerance in host master actor
      val result = Try(worker.test).toOption.flatten
      sender ! TestResult(result)
    case Test => log.info("Test is disabled, skipping tests.")
    // forward messages, used for stop system after all tests are finished
    case Forward(actor, message) => actor.tell(message, sender)
  }

  override def postStop = closeWorker.foreach(_.apply)
}
