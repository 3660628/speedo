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

import akka.actor.{ Actor, ActorLogging }

import com.twitter.scalding.Args

import com.htc.speedo.caffe.CaffeWorker._

/**
 * An abstract actor with training parameters. This is the base actor for [[DBActor]] and [[WorkerActor]].
 *
 * Optional parameters:
 *  - `--sync`: The same flag used to determine synchronous master actor. With synchronous master
 * actor, all workers read snapshot from global key in storehaus, otherwise read from key with suffix.
 *  - `--gradientOnly`: (flag) If provided, the workers will only calculate the gradients and the db
 * actor is responsible to calculate velocity and weights based on the gradients. Also affacts the
 * behavior of db actor. This can work with all types of master actor strategies.
 *  - `--movingRate`: If provided, each worker has its own clock when workers update their local
 * weights. The master performance an update whenever the local workers finished t steps of their
 * gradient updates. Magnitude of movingRate/learningRate represents the amount of exploration we
 * allow in the model. Smaller of which allows for more exploration as it allows worker fluctuating
 * further from the center. Due to the existence of local optima of non-convex problem, we want for
 * more exploration.
 * @param args The command line arguments to create actors. Usually is provided by [[HostMasterActor]].
 * @note All the parameters can be updated with [[UpdateParameter]] message
 * @note If running with synchronous master actor, `--movingRate` is ignored.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 * @author Wenrui Jiang (roy_jiang@htc.com)
 */
abstract class ParameterActor(args: Args) extends Actor with ActorLogging {
  // The default value of the following parameters are not important
  // They will be updated by [[updateParameter]] below.

  /** If running with synchronous master actor or not (`--sync` flag). */
  var synchronous: Boolean = false
  /** Moving rate for EASGD (i.e. [[SelfPaceFullUpdate]]) */
  var movingRate: Option[Float] = None
  /** The weight update strategy to use in [[CaffeWorker.train]]. */
  var weightUpdate: WeightUpdate = FullUpdate

  // Actual initialization of the vars goes here
  updateParameter(args)

  /**
   * Update the parameters based on given arguments. Should be called upon
   * receiving [[UpdateParameter]] message.
   */
  def updateParameter(args: Args): Unit = {
    synchronous = args.boolean("sync")
    movingRate = args.optional("movingRate").map(_.toFloat)
    weightUpdate = movingRate match {
      case Some(_) if !synchronous => SelfPaceFullUpdate
      case _ => args.boolean("gradientOnly") match {
        case true => GradientOnly
        case false => FullUpdate
      }
    }
  }
}
