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

import com.twitter.util.Await

import org.specs2.mutable.SpecificationWithJUnit

/**
 * Unit test for [[HDFSStore]].
 *
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
class HDFSStoreSpec extends SpecificationWithJUnit {
  sequential

  val key = "key"
  val value1 = 1
  val value2 = 2

  "HDFSStore" should {
    val store = HDFSStore[Int]("target/hdfsstore-test")

    "get None for non-existance keys" in {
      Await.result(store.get(key)) must beNone
    }
    "put and get correctly" in {
      Await.result(store.put(key, Some(value1)))
      Await.result(store.get(key)).get must_== value1
    }
    "override existing keys with put correctly" in {
      Await.result(store.put(key, Some(value2)))
      Await.result(store.get(key)).get must_== value2
    }
    "delete existing keys correctly" in {
      Await.result(store.put(key, None))
      Await.result(store.get(key)) must_== None
    }
    "delete non-existing keys as no-op" in {
      Await.result(store.put(key, None))
      Await.result(store.get(key)) must_== None
    }
    step { store.close() }
  }
}
