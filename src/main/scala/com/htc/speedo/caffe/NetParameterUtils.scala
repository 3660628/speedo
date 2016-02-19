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

import scala.util.Random

import caffe.Caffe.NetParameter

import com.twitter.algebird.Semigroup

/**
 * Utility of caffe network parameters semigroup.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
object NetParameterUtils {
  /** Semigroup for caffe network in protobuf. */
  def semigroupProto(skip: Float = 0f): Semigroup[NetParameter] =
    if (skip == 0)
      new Semigroup[NetParameter] {
        override def plus(p1: NetParameter, p2: NetParameter) = NetParameterOperation.plus(p1, p2)
      }
    else
      new Semigroup[NetParameter] {
        override def plus(p1: NetParameter, p2: NetParameter) = NetParameterOperation.plus(p1, p2, skip)
      }

  /** Semigroup for caffe network in binary. */
  def semigroup(skip: Float = 0f): Semigroup[Array[Byte]] =
    if (skip == 0)
      new Semigroup[Array[Byte]] {
        override def plus(p1: Array[Byte], p2: Array[Byte]) = NetParameterOperation.plus(p1, p2)
      }
    else
      new Semigroup[Array[Byte]] {
        override def plus(p1: Array[Byte], p2: Array[Byte]) = NetParameterOperation.plus(p1, p2, skip)
      }
}
