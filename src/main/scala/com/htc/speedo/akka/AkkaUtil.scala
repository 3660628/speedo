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

import scala.collection.mutable.Buffer

import akka.actor._

import com.twitter.scalding.Args
import com.typesafe.config.ConfigFactory

/**
 * Provides utilities for creating akka system and host actors.
 *
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
object AkkaUtil {
  /** The command line flag of host. */
  val hostFlag = "host"

  /** An akka extension to get system address. See http://stackoverflow.com/questions/14288068 */
  class AddressExtension(system: ExtendedActorSystem) extends Extension {
    // The address of the akka system (contains remote information)
    val address = system.provider.getDefaultAddress
  }
  /** A companion object for easier creating extensions from actor system. */
  object AddressExtension extends ExtensionKey[AddressExtension]

  /**
   * Return the external address of the actor system, including protocol, hostname and port if
   * remoting is enbabled.
   */
  def addressOf(system: ActorSystem): Address = AddressExtension(system).address

  /**
   * Create an akka system which supports tcp remoting from the given arguments.
   *
   * Arguments:
   *  - `--host <name/ip>`: The host name or ip to listen to.
   *  - `--port <port>`: (optional) The port number to start akka system.
   * Default is 0, i.e. a random available port. To get port after system started, use [[addressOf]].
   */
  def createSystem(args: Args): ActorSystem = {
    val hostname = args.required(hostFlag)
    val port = args.int("port", 0)
    val configString = s"""akka {
      actor { provider = "akka.remote.RemoteActorRefProvider" }
      remote {
        enabled-transports = ["akka.remote.netty.tcp"]
        netty.tcp {
          hostname = "$hostname"
          port = $port
        }
      }
    }
    $WorkerDispatcher {
      executor = "thread-pool-executor"
      type = PinnedDispatcher
    }"""
    ActorSystem(SystemName, ConfigFactory.parseString(configString).withFallback(ConfigFactory.load))
  }

  /**
   * Create the host actor in the given actor system. If a `--master` parameter is given, a
   * [[HostSlaveActor]] is created, otherwise [[HostMasterActor]] is created.
   *
   * Arguments for slave system:
   * - `--master <master host/ip>`: The host name or ip of the master actor system to connect to.
   * The host name or ip must be the same with that passed to [[createSystem]] on the master machine.
   * - `--worker <Number of workers>`: (Optional) If provided, start multiple worker actor in one
   * slave system.
   */
  def createHostActor(args: Args, system: ActorSystem): ActorRef = {
    // Slave host actors need to specify which master to connect to
    val hostActorClass = args.boolean("master") match {
      case false => classOf[HostMasterActor]
      case true => classOf[HostSlaveActor]
    }
    // exclude `--host` argument
    system.actorOf(Props(hostActorClass, new Args(args.m - hostFlag)), hostFlag)
  }

  /**
   * Create the master actor props. According to akka document, the props is recommended to be
   * created outside of an actor class, if use `Props(new XXXActor)` syntax. We have to use this
   * syntax other than `Props(classOf[XXXAxtor], args...)` since we need to mix-in traits.
   */
  def createMasterActorProps(args: Args, db: ActorRef, tester: ActorRef, workers: Seq[ActorRef]): Props = {
    val buffer = workers match {
      // use a clone of workers buffer, since it's modified in master actor
      case b: Buffer[ActorRef] => b.clone
      // otherwise just create a new buffer
      case _ => workers.toBuffer
    }
    Props(
      if (args.boolean("hybrid")) // hybrid must be first
        new HybridMasterActor(args, db, tester, buffer)
      else if (args.boolean("weedout"))
        new MasterActor(args, db, tester, buffer) with WeedOutMasterActor
      else if (args.boolean("maxAdvance"))
        new MasterActor(args, db, tester, buffer) with PSMasterActor
      else if (args.boolean("sync"))
        new MasterActor(args, db, tester, buffer) with SynchronousMasterActor
      else new MasterActor(args, db, tester, buffer)
    )
  }

  /**
   * A main utility to start master/slave akka system and host actors.
   *
   * Common Parameters:
   *  - `--host <name/ip>`: The host name or ip to listen to.
   *  - `--port <port>` The port to listen to. Default is random.
   *
   * Parameters for worker:
   * - `--master <master host/ip>`: The host name or ip of the master actor system to connect to.
   * - `--worker <Number of workers>`: (Optional) If provided, start multiple worker actor in one
   * slave system. Useful for multi-GPU machine, as each worker can utilize a different GPU.
   * @note Multiple workers in one akka system is only enabled when running slaves manually. When
   * running on yarn, always start one worker per system.
   * @see [[HostMasterActor]] for other parameters needed for master.
   */
  def main(arg: Array[String]): Unit = {
    val args = Args(arg)
    createHostActor(args, createSystem(args))
  }
}
