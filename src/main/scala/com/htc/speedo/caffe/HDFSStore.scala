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

import com.twitter.bijection.Codec
import com.twitter.storehaus.Store
import com.twitter.util.{ Future, Time }

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object HDFSStore {
  /** Create a [[HDFSStore]] from a string path. */
  def apply[V: Codec](rootDir: String): HDFSStore[V] = HDFSStore(new Path(rootDir), new Configuration)
}

/**
 * A HDFS store for caffe workers. Each key value pair is stored as a file, with the key as filepath
 * and value as the file contents. Therefore, it's recommended to use this store for large values,
 * e.g. for caffe network with more than 100M snapshots.
 * TODO: solve the problem of con-current read and write
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
case class HDFSStore[V](rootDir: Path, conf: Configuration = new Configuration)(implicit codec: Codec[V])
    extends Store[String, V] {
  /** The file system for the root path. */
  val fs = rootDir.getFileSystem(conf)

  // make sure the root directory exists
  fs.mkdirs(rootDir)

  override def get(key: String) = Future {
    Some(new Path(rootDir, key)).filter(fs.exists).flatMap { path =>
      val stream = fs.open(path)
      val bytes = IOUtils.toByteArray(stream)
      stream.close
      codec.invert(bytes).toOption
    }
  }

  override def put(kv: (String, Option[V])) = Future {
    val path = new Path(rootDir, kv._1)
    kv._2 match {
      case None => fs.delete(path, false)
      case Some(v) =>
        val bytes = codec(v)
        val stream = fs.create(path, true)
        stream.write(bytes)
        stream.close
    }
  }

  override def close(time: Time) = Future { fs.close }
}
