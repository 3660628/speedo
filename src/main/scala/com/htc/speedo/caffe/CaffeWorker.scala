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

package com.htc.speedo.caffe

import java.io.File

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.{ Buffer, Set => MutableSet }

import caffe.Caffe.{ NetParameter, Phase, SolverParameter }

import com.google.protobuf.Message
import com.twitter.scalding.Args
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.Await

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object CaffeWorker {
  /**
   * A basic trait for how to update weights, i.e. what to write to storehaus in
   * [[CaffeWorker.train]].
   */
  sealed trait WeightUpdate
  /**
   * Write the full weight different against snapshot to storehaus, including
   * gradients, momentum and weight decay, based on the solver update function.
   */
  case object FullUpdate extends WeightUpdate
  /**
   * Update the weight in each worker's own pace, in which case, maintains a
   * local copy of weight. See EASGD paper for details.
   */
  case object SelfPaceFullUpdate extends WeightUpdate
  /**
   * Write grandients to the storehaus store. The gradients can be merged by
   * calling [[mergeDelta]] separately.
   */
  case object GradientOnly extends WeightUpdate

  /** The default work directory for caffe. */
  val BaseDir = new File("caffe.worker")

  /**
   * Create a caffe worker from command line arguments.
   * See constructor for parameter details.
   * @note The storehaus store will be created using [[StorehausUtils]]. Make
   * sure to close the caffe worker in your code.
   */
  def apply(args: Args): CaffeWorker = CaffeWorker(
    StorehausUtils.createStore(args),
    args.required("solver"),
    args.optional("snapshot"),
    args.optional("keyName"),
    args.optional("suffix"),
    args.optional("baseDir").map(new File(_)),
    args.boolean("debug"),
    args.optional("device").map(_.toInt),
    args.boolean("doublePrecision"),
    args.optional("train").map(_.toInt),
    args.optional("lr").map(_.toFloat),
    args.optional("weightDecay").map(_.toFloat),
    args.optional("momentum").map(_.toFloat),
    args.optional("batch").map(_.toInt),
    args.optional("start").map(_.toFloat)
  )
}

/**
 * Caffe worker to invoke caffe training.
 * Training inputs come from hdfs and the network parameters are merged into
 * a storehaus store.
 *
 * TODO: Instead of passing in all the parmaeters, use configuration.
 *
 * @note All parameters (except snapshotStore) can be set through scalding args
 * using [[apply]] function in companion object. The argument is same with the
 * variable name, except stated explicitly.
 * @param snapshotStore The snapshot store used to merge network parameter.
 * Should use [[NetParameterSemigroup]] as semigroup for merge.
 * @param solverPath The path to a text format of SolverParameter. The
 * definition of model must be specified in a file in `net` field. The command
 * line argument is `--solver`.
 * @param snapshotPath The path to a binary format of NetParameter used as a
 * snapshot to resume train from. The command line argument is `--snapshot`.
 * @param keyName The name used as key in the database, overriding the name in
 * the model file.
 * @param suffix The worker will read/write it's snapshot/delta using a separate
 * key, the new key will be `<original key> + suffix`.
 * @param baseDirectory Overrides working directory for caffe. Relative to
 * current directory. The command line argument is `--baseDir`.
 * @param debug If set to true, display the loss every iteration in caffe
 * @param device The GPU device index to use for this caffe worker. A negative
 * index indicates using CPU; positive device index is MOD by total available
 * GPU counts. E.g. non-negative GPU indexes are identical on one-GPU machine.
 * On a machine without GPU, device is ignored.
 * @param doublePrecision Use double precision floating number in the solver.
 * This may be more accurate but consumes double memory. Default is false.
 * @param trainIteration Overrides the number of mini-batch to run in training.
 * The command line argument is `--train`. This defaults to 1.
 * @param baseLR Overrides the base learning rate. The command line argument is
 * `--lr`.
 * @param weightDecay Overrides the weight decay.
 * @param momentum Overrides the momentum.
 * @param batchSize Overrides the batch size of all training data layers. The
 * command line argument is `--batch`. For back compatibility, this option is
 * named as batch size, but it actually operates on the `iter_size` field in
 * solver, since changing batch size of data layers during runtime requires a
 * lot of hacks and is not stable. For easier usage, if all the data layers has
 * the same batch size, their batch size are changed to 1 and iter_size is
 * multiplied by the original batch size during initialization.
 * @param start Overrides start position of input data for the worker. This
 * should be a float value, equals to <start position>/<total number of data>.
 * You should also set the `rand_skip` field in the protobuf to be the total
 * number of data.
 * @note Users should be responsible for closing the store themselves.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class CaffeWorker(
    snapshotStore: MergeableStore[String, Array[Byte]],
    solverPath: String,
    snapshotPath: Option[String] = None,
    keyName: Option[String] = None,
    suffix: Option[String] = None,
    baseDirectory: Option[File] = None,
    debug: Boolean = false,
    device: Option[Int] = None,
    doublePrecision: Boolean = false,
    // override default config from command line
    trainIteration: Option[Int] = None,
    baseLR: Option[Float] = None,
    weightDecay: Option[Float] = None,
    momentum: Option[Float] = None,
    batchSize: Option[Int] = None,
    start: Option[Float] = None
) {
  val baseDir = baseDirectory.getOrElse(CaffeWorker.BaseDir)

  // Get the solver parameter, jni solver and the name of the network
  private val (param, solver, name) = {
    // hdfs path of the solver file
    val path = new Path(solverPath)
    // hdfs fodler of the solver file
    val pathDir = path.getParent
    val fs = path.getFileSystem(new Configuration)
    // load solver parameter from hdfs
    val solverBuilder = ProtobufUtils.loadText[SolverParameter](fs.open(path)).toBuilder
    // load net parameter from hdfs, relative to solver parameter path
    val modelBuilder = ProtobufUtils.loadText[NetParameter](fs.open(new Path(pathDir, solverBuilder.getNet))).toBuilder
    // make sure local directory exists and is empty
    if (baseDir.exists) FileUtils.deleteDirectory(baseDir)
    // copy the whole directory to local
    val localPath = new Path(baseDir.toURI)
    fs.copyToLocalFile(false, pathDir, localPath, true)
    // change the iteration settings in the solver
    solverBuilder
      // if debug, display loss information every iteration
      .setDisplay(if (debug) 1 else 0)
      // don't run tests
      .setTestInitialization(false)
      .setTestInterval(Int.MaxValue)
      // don't save snapshots during training
      .clearSnapshot
      // don't save snapshot after training
      .setSnapshotAfterTrain(false)
      // clear networks definitions in the solver
      .clearTrainNet
      .clearTestNet
      .clearTrainNetParam
      .clearTestNetParam
      .clearNetParam
      // set training iteration before merge, always override value in solver
      .setMaxIter(trainIteration.getOrElse(1))
    baseLR.foreach(solverBuilder.setBaseLr)
    weightDecay.foreach(solverBuilder.setWeightDecay)
    momentum.foreach(solverBuilder.setMomentum)

    // A helper function to change paths in the net definition
    def pathPrefix(path: String): String =
      // if path is already absolute, do nothing. This is useful for large
      // datasets, so we don't need to copy from hdfs everytime
      if (path.startsWith("/")) path
      // is the path is relative, then it's relative to the baseDir
      else baseDir.getName + "/" + path

    // change the input file path, batch size and start pos in the data layers
    // TODO: Add unit test
    val batchSizeSet = MutableSet[Int]()
    val batchSetter = Buffer[Int => Message.Builder]()
    modelBuilder.getLayerBuilderList.asScala.foreach { l =>
      // Only change batch size for train net
      val includes = l.getIncludeList.asScala.filter(_.hasPhase)
        .map(_.getPhase) ++ Some(l).filter(_.hasPhase).map(_.getPhase)
      val excludes = l.getExcludeList.asScala.filter(_.hasPhase).map(_.getPhase)
      val isTrainNet = (includes.size == 0 || includes.contains(Phase.TRAIN)) &&
        (excludes.size == 0 || !excludes.contains(Phase.TRAIN))
      if (l.hasTransformParam) {
        val transformParam = l.getTransformParamBuilder
        if (transformParam.hasMeanFile)
          transformParam.setMeanFile(pathPrefix(transformParam.getMeanFile))
      }
      if (l.hasDataParam) {
        val dataParam = l.getDataParamBuilder
        if (dataParam.hasSource)
          dataParam.setSource(pathPrefix(dataParam.getSource))
        if (dataParam.hasMeanFile)
          dataParam.setMeanFile(pathPrefix(dataParam.getMeanFile))
        // Only set start position for train net
        start.filter(_ => isTrainNet).foreach { s =>
          if (dataParam.hasRandSkip)
            dataParam.setRandSkip((dataParam.getRandSkip * s).toInt)
          else if (s > 0)
            throw new Exception("rand_skip must be set as total number of" +
              "training data in data layers for multiple workers")
        }
        if (isTrainNet) {
          batchSizeSet += dataParam.getBatchSize
          batchSetter += dataParam.setBatchSize
        }
      }
      if (l.hasHdf5DataParam) {
        val hdf5DataParam = l.getHdf5DataParamBuilder
        if (hdf5DataParam.hasSource)
          hdf5DataParam.setSource(pathPrefix(hdf5DataParam.getSource))
        if (isTrainNet) {
          batchSizeSet += hdf5DataParam.getBatchSize
          batchSetter += hdf5DataParam.setBatchSize
        }
      }
      if (l.hasImageDataParam) {
        val imageDataParam = l.getImageDataParamBuilder
        if (imageDataParam.hasSource)
          imageDataParam.setSource(pathPrefix(imageDataParam.getSource))
        if (imageDataParam.hasMeanFile)
          imageDataParam.setMeanFile(pathPrefix(imageDataParam.getMeanFile))
        // Only set start position for train net
        start.filter(_ => isTrainNet).foreach { s =>
          if (imageDataParam.hasRandSkip)
            imageDataParam.setRandSkip((imageDataParam.getRandSkip * s).toInt)
          else if (s > 0)
            throw new Exception("rand_skip must be set as total number of" +
              "training data in data layers for multiple workers")
        }
        if (isTrainNet) {
          batchSizeSet += imageDataParam.getBatchSize
          batchSetter += imageDataParam.setBatchSize
        }
      }
      if (l.hasMemoryDataParam && isTrainNet) {
        val memoryDataParam = l.getMemoryDataParamBuilder
        batchSizeSet += memoryDataParam.getBatchSize
        batchSetter += memoryDataParam.setBatchSize
      }
      if (l.hasWindowDataParam) {
        val windowDataParam = l.getWindowDataParamBuilder
        if (windowDataParam.hasSource)
          windowDataParam.setSource(pathPrefix(windowDataParam.getSource))
        if (windowDataParam.hasMeanFile)
          windowDataParam.setMeanFile(pathPrefix(windowDataParam.getMeanFile))
        if (isTrainNet) {
          batchSizeSet += windowDataParam.getBatchSize
          batchSetter += windowDataParam.setBatchSize
        }
      }
    }
    // update batch sizes if explicit override or all batch sizes are same
    batchSize.orElse(batchSizeSet.size match {
      // multiplies by the iter_size in case it's not 1
      case 1 => batchSizeSet.headOption.map(_ * solverBuilder.getIterSize)
      case _ => None
    }).foreach { batch =>
      // set batch size to 1
      batchSetter.foreach(_(1))
      // multiply iter_size by original batch size
      solverBuilder.setIterSize(batch)
    }
    // set device (Only set device if we have GPUs)
    if (Solver.deviceCount > 0) device match {
      case Some(d) if d >= 0 => Solver.setDevice(d % Solver.deviceCount) // GPU
      case Some(_) => Solver.setDevice(-1) // CPU
      case _ => // Do nothing is device is not explicitly set
    }
    val jniSolver = new Solver
    val solverParam = solverBuilder.build
    jniSolver.init(solverParam, modelBuilder.build, doublePrecision)
    // load snapshot
    snapshotPath.foreach { path =>
      // convert relative path to absolute path
      val snapshot = ProtobufUtils.load[NetParameter](pathPrefix(path))
      jniSolver.setWeight(snapshot.toByteArray)
    }
    // return solver parameter, jni solver and network name
    (solverParam, jniSolver, keyName.getOrElse(modelBuilder.getName))
  }

  /** Returns the name of storehaus key. */
  def getKey: String = name

  /**
   * Put the initial weight to store if the key is not available.
   * @param resume If set to true, the snapshot in storehaus store is used.
   * @return The weights after initialization (or the one already exists)
   */
  def init(resume: Boolean = false): Array[Byte] = {
    val snapshot = resume match {
      // always override the snapshot
      case false => None
      // fetch the snapshot
      case true => Await.result(snapshotStore.get(name))
    }
    snapshot.getOrElse {
      val weights = solver.getWeight
      Await.result(snapshotStore.put(name, Some(weights)))
      weights
    }
  }

  /**
   * Trains the caffe model for a small amount of iterations.
   * @param readSuffix Whether read snapshot from the key with suffix or not.
   * Default is true.
   * @param WeightUpdate The enum of how to update the weights and update
   * storehuas. See [[CaffeWorker.WeightUpdate]] and its sub case objects.
   * Default is [[CaffeWorker.FullUpdate]].
   */
  def train(readSuffix: Boolean = true, WeightUpdate: CaffeWorker.WeightUpdate = CaffeWorker.FullUpdate): Double = {
    // If suffix is provided and readSuffix is enabled,
    // fetch weight snapshot from the key with suffix
    val snapshot = suffix.filter(_ => readSuffix)
      .map(suf => Await.result(snapshotStore.get(name + suffix.get))).flatten
      // fall back to key without suffix
      .orElse(Await.result(snapshotStore.get(name)))
      // snapshot must not be empty
      .getOrElse(throw new Exception("Snapshot is empty!"))
    // set snapshot
    solver.setWeight(snapshot)
    // run caffe train and get the delta
    val (loss, delta) = WeightUpdate match {
      case CaffeWorker.FullUpdate =>
        (solver.train(param.getMaxIter, true), NetParameterOperation.minus(solver.getWeight, snapshot))
      case CaffeWorker.SelfPaceFullUpdate =>
        (solver.train(param.getMaxIter, true), solver.getWeight)
      case CaffeWorker.GradientOnly => (solver.train(1, false), solver.getDelta)
    }
    suffix match {
      case None =>
        // merges the delta to store if no suffix given
        Await.result(snapshotStore.merge(name, delta))
      case Some(suf) =>
        Await.result(snapshotStore.put(name + suf, Some(delta)))
    }
    // return loss
    loss
  }

  /** Tests the caffe model for all test data. Returns accuracy. */
  def test: Option[Double] =
    // fetch a snapshot
    Await.result(snapshotStore.get(name)).map { weights =>
      // set the snapshot in solver
      solver.setWeight(weights)
      // run caffe test
      solver.test(param.getTestIter(0))
    }

  /** Set current global iteration for the worker. */
  def setIteration(iteration: Int): Unit = solver.setIteration(iteration)

  /** Updates weights according to the given gradients and write to store. */
  def mergeDelta(delta: Array[Byte], suffix: String = ""): Array[Byte] = {
    val weights = solver.mergeDelta(delta)
    Await.result(snapshotStore.put(name + suffix, Some(weights)))
    weights
  }

  /**
   * Update solver parameters using command line arguments. Shares same
   * arguments as the [[CaffeWorker.apply]] function.
   *
   * Supported options:
   *  - `--momentum`: Momentum
   *  - `--weightDecay`: Weight decay
   *  - `--lr`: Base learning rate
   *  - `--batch`: Update the iter_size in the solver.
   * @note If the any option is not provided in the command line, it's reset to
   * the original one defined in solver protobuf or model protobuf (batch size).
   */
  def updateParameter(args: Args): Unit = {
    val builder = SolverParameter.newBuilder
    args.optional("lr").map(_.toFloat).foreach(builder.setBaseLr)
    args.optional("weightDecay").map(_.toFloat).foreach(builder.setWeightDecay)
    args.optional("momentum").map(_.toFloat).foreach(builder.setMomentum)
    args.optional("batch").map(_.toInt).foreach(builder.setIterSize)
    solver.updateParameter(builder.build)
  }

  /** Finalize resources and close storehaus store. */
  def close: Unit = {
    solver.dispose
    Await.result(snapshotStore.close())
  }
}
