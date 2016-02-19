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

import org.jboss.netty.buffer.{ ChannelBuffer, ChannelBuffers }

import com.twitter.bijection.Injection
import com.twitter.bijection.netty.ChannelBufferBijection
import com.twitter.finagle.redis.Client
import com.twitter.scalding.Args
import com.twitter.storehaus.{ JMapStore, Store }
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.storehaus.redis.RedisStore

/**
 * Utility of creating storehaus.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
object StorehausUtils {
  /** Create mergeable store from command line arguments. */
  def createStore(args: Args): MergeableStore[String, Array[Byte]] = {
    val store = args.optional("redis").map { host => // create redis store
      val client = Client("%s:%d" format (host, 6379))
      implicit val inj = Injection.fromBijection(ChannelBufferBijection.inverse)
      Store.convert(RedisStore(client)) { str: String => ChannelBuffers.copiedBuffer(str.getBytes) }
    }.orElse(args.optional("hdfs").map { path => // create hdfs store
      HDFSStore[Array[Byte]](path)
    }).getOrElse(new JMapStore[String, Array[Byte]]) // create memory store by default
    implicit val semigroup = NetParameterUtils.semigroup(args.getOrElse("skip", "0").toFloat)
    MergeableStore.fromStore(store)
  }
}
