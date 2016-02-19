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

import scala.collection.mutable.{ Buffer, Map => MutableMap }
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.Try

import akka.AkkaException
import akka.actor._
import akka.actor.SupervisorStrategy.{ Restart, Resume, Stop }
import akka.remote.RemoteScope

import com.twitter.scalding.Args

import HostMasterActor._

/**
 * A host master actor is used to wait for all slaves to join. It's the creator of all actors
 * (worker, db, test and master).
 *
 * Arguments:
 *  - `--worker <#workers>`: The number of workers.
 *  - '--resume': (Optional) Snapshot in the store is used as initial weight.
 * Otherwise current weights in the caffe solver is used as initial weight.
 *  - '--snapshot <snapshot path>': (Optional) Use a snapshot in file system as initial weights. The
 * snapshot will be loaded during initialization, `resume` is set to false. See [[com.htc.speedo.caffe.CaffeWorker CaffeWorker]].
 *  - '--CPUDBActor': (Flag) If set, forces db actor and test actor to use CPU not GPU.
 *
 * Arguments for Yarn (Normally don't needed when start manually):
 *  - '--timeout <seconds>': (Optional) Set the waiting time out for all the slave systems to join.
 * Default is no time out. Usually not needed when running the system manually. It's set to 30
 * seconds if started by Yarn.
 *  - `--sleepAfterFinish <seconds>`: (Optional) Sleeps given seconds before shutdown the system.
 * This is set by Yarn to 15 seconds if `--test` is 0.
 *
 * Parameters used by different type of master actor:
 *  - `--drop <threshold>`: The strategy that drops slow training iterations.
 *  - `--maxAdvance <shift>`: The strategy that waits for slow iterations.
 *  - `--sync`: The completely synchronous strategy.
 *  - None: The completely asynchronous strategy.
 * @see Other parameters required by [[MasterActor]] for master actor.
 * @see Other parameters required by [[com.htc.speedo.caffe.CaffeWorker CaffeWorker]] for caffe parameters.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class HostMasterActor(args: Args) extends Actor with ActorLogging {
  /** Number of workers. */
  val numWorkers = args.int("worker")
  /** Time out for waiting all the host actors to join. */
  val timeOut = args.int("timeout", 0)
  /**
   * The flag to resume or not. If `--snapshot` is provided, the snapshot is loaded in the caffe
   * worker when DBActor is created. So resume is false.
   */
  val resume = args.boolean("resume") && !args.boolean("snapshot")
  /* The device index for DB actor. Equals to -1 if CPUDBActor flag is given, otherwise 0. */
  val dbDevice = if (args.boolean("CPUDBActor")) -1 else 0

  if (timeOut > 0) {
    log.info("Waiting {} seconds for {} workers to join.", timeOut, numWorkers)
    implicit val dispatcher = context.dispatcher
    context.system.scheduler.scheduleOnce(timeOut.seconds, self, JoinTimeOut)
  } else log.info("Waiting for {} workers to join.", numWorkers)

  /** The buffer for all the host slave actors on remote machines. */
  val hostActors: Buffer[ActorRef] = Buffer()
  /** Accumulates count for different host names/ips. */
  val hostCounts = MutableMap(getHost(AkkaUtil.addressOf(context.system)) -> (dbDevice + 1))

  /**
   * The buffer for all the worker actors on remote machines. A worker actor is created once a
   * remote system joins to this actor.
   */
  val workerActors: Buffer[ActorRef] = Buffer()
  /** The master actor. Only created after all worker actors are created. */
  var masterActor: Option[ActorRef] = None
  /** The db actor. */
  val dbActor = createDbActor
  context watch dbActor
  /**
   * The tester actor. Mark as lazy so that test actor is not created at the
   * same time with dbActor. This avoids creating two caffe solver at the same
   * time, which might cause seg fault on GPU.
   * @note The test actor is not watched during init and train, since it's not
   * a must in these two phases. But after training, we start watch the actor
   * since it's required in the shutdown process. If the actor is terminated
   * beforehand, we can still receive the Terminated message by then.
   */
  lazy val testActor = createTester

  /** Create the master actor. */
  def createMasterActor: ActorRef =
    context.actorOf(AkkaUtil.createMasterActorProps(args, dbActor, testActor, workerActors), "master")

  /** Create the database actor. */
  def createDbActor: ActorRef =
    context.actorOf(Props(classOf[DBActor], argsWithDevice(dbDevice)).withDispatcher(WorkerDispatcher), "db")

  /** Create `index`th worker actor in the remote system at given address. */
  def createWorker(index: Int, address: Address): ActorRef = {
    val name = workerPrefix + index
    // The start proportion of the input data
    val start = (index.toFloat / numWorkers).toString
    // get device index for this worker
    val device = hostCounts.getOrElse(getHost(address), 0)
    // update the host count
    hostCounts.put(getHost(address), device + 1)
    // args for worker actor, use different base dir for each worker in case
    // multiple workers start in the same jvm
    val newArgs = argsWithDevice(device) + ("suffix" -> Seq(name)) +
      ("start" -> Seq(start)) + ("baseDir" -> Seq("caffe." + name))
    // remote deployment
    val props = Props(classOf[WorkerActor], newArgs)
      .withDeploy(Deploy(scope = RemoteScope(address)))
      .withDispatcher(WorkerDispatcher)
    context.actorOf(props, name)
  }

  /** Create the test actor. No need to set device as it's set by db actor. */
  def createTester: ActorRef =
    context.actorOf(Props(classOf[WorkerActor], args).withDispatcher(WorkerDispatcher), "tester")

  /** Create a new scalding args from [[args]] with given device flag. */
  def argsWithDevice(device: Int): Args = args + ("device" -> Seq(device.toString))

  /** Get host name/ip from given akka address. Return localhost if empty. */
  def getHost(address: Address): String = address.host.getOrElse("localhost")

  /** Message handling during initialization, training and post-train. */
  def commonState: Receive = {
    case Progress => masterActor match {
      // forward message to master actor
      case Some(master) => master.tell(Progress, sender)
      // master actor is not started (still waiting for remote systems to join)
      case None => sender ! Progress(0)
    }
    case StopAkka => // shutdown the whole system
      // stop all slaves first
      hostActors.foreach(_ ! StopAkka)
      args.int("sleepAfterFinish", 0) match {
        // stop master akka system immediately
        case 0 => context.system.shutdown
        // schedule the shutdown after i seconds
        case i: Int =>
          log.info("System will be shutdown in {} seconds.", i)
          context.system.scheduler.scheduleOnce(i.seconds)(context.system.shutdown)(context.dispatcher)
      }
    case Terminated(`dbActor`) =>
      log.warning("Db actor stopped! Shutdown the system!")
      self ! StopAkka
    case Terminated(`testActor`) =>
      log.warning("Test actor stopped! Shutdown the system!")
      self ! StopAkka
  }

  /** Message handling during initialization, not training or post-train. */
  def initState: Receive = {
    // This happens when not all the workers start within given timeout
    case JoinTimeOut =>
      log.error("Failed to start all workers within {} seconds!" + " Shutdown the system!", timeOut)
      self ! StopAkka
  }

  /** Message handling during initialization and training, not post-train. */
  def initOrTrainingState: Receive = {
    case Join =>
      // A remote system joins with its address
      val address = sender.path.address
      log.info("Creating worker{} at {}", hostActors.size, address)
      // creates worker at the remote system
      val worker = createWorker(hostActors.size, address)
      // make sure the worker is created (will receive ActorIdentity message)
      worker ! Identify(IdentifyWorker(worker))
      hostActors += sender
    case ActorIdentity(IdentifyWorker(ref), Some(worker)) if ref == worker =>
      log.info("{} created!", worker.path.name)
      // If a worker is created successfully
      context watch worker
      workerActors += worker
      // clears worker's suffix key
      if (!resume) dbActor ! ClearWorkerKey(worker)
      masterActor match {
        case None if workerActors.size == numWorkers => // all workers joined
          // create and watch master actor
          masterActor = Some(createMasterActor)
          masterActor.foreach { actor =>
            context watch actor
            // The master actor should start after dbActor finished init
            dbActor.tell(Init(resume), actor)
          }
          // Cheange message handling
          context.become(commonState orElse initOrTrainingState orElse trainingState)
        case Some(master) => // re-created workers
          // inform master about newly created workers
          // go through db actor to make sure the suffix key is cleared first
          dbActor ! Forward(master, WorkerCreated(worker))
        case _ => // do nothing if we are still waiting for workers to join
      }
    case ActorIdentity(IdentifyWorker(worker), _) =>
      // If a worker is not created successfully
      // TODO: continue without this worker
      log.error("Failed to create {}, stop akka!", worker)
      self ! StopAkka
    case Terminated(w) if workerActors.contains(w) => // worker stopped
      log.info("{} stopped!", w.path.name)
      workerActors -= w
      // Check if host actor is stopped or not, try to restart worker
      // We can get the corresponding host actor from index in worker's name
      // But this may fail in unit tests, so we wrap it with a Try
      Try(w.path.name.stripPrefix(workerPrefix).toInt).toOption.map { index =>
        val host = hostActors(index)
        host ! Identify(IdentifyHost(host))
      }
    case ActorIdentity(IdentifyHost(ref), Some(host)) if ref == host =>
      // If the host is still running after worker is terminated, restart worker
      val address = host.path.address
      val index = hostActors.indexOf(host)
      log.info("Re-creating worker{} at {}", index, address)
      // creates worker at the remote system again
      val worker = createWorker(index, address)
      worker ! Identify(IdentifyWorker(worker))
    case ActorIdentity(IdentifyHost(host), _) =>
      // If both worker and host actor are stopped, not restart worker
      // prepare shutdown if all workers stopped
      if (workerActors.size == 0) self ! StopAkka
  }

  /** Message handling during training, not initialization or post-train. */
  def trainingState: Receive = {
    case TrainFinished =>
      log.info("Train finished, starting test.")
      // start to watch test actor
      context.watch(testActor)
      // run test after master finished training and log test result in master
      testActor.tell(Test, sender)
      // stop the system after test finished
      testActor ! Forward(self, StopAkka)
      // unwatch workers to make log clean
      workerActors.foreach(context.unwatch)
      // unwatch master
      masterActor.foreach(context.unwatch)
      // do not react to certain messages
      context.become(commonState)
    case Terminated(actor) if masterActor == Some(actor) =>
      log.warning("Master actor stopped! Shutdown the system!")
      self ! StopAkka
  }

  override def receive = commonState orElse initState orElse initOrTrainingState

  // always stop the child actors when error occured
  override val supervisorStrategy = OneForOneStrategy() { case _ => Stop }
}

object HostMasterActor {
  case class IdentifyHost(host: ActorRef)
  case class IdentifyWorker(worker: ActorRef)
  val workerPrefix = "worker"
}
