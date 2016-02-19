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

import java.io.{ InputStream, InputStreamReader }
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{ Files, Paths }

import com.google.protobuf.{ Message, MessageOrBuilder, TextFormat }

/**
 * Utility to serialize/deserialize protobuf messages in text and binary format. Compact with caffe.
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
object ProtobufUtils {
  /** Loads a protobuf message in binary format from a local file. */
  def load[M <: Message](path: String)(implicit mf: Manifest[M]): M =
    mf.runtimeClass.getDeclaredMethod("parseFrom", classOf[Array[Byte]])
      .invoke(null, Files.readAllBytes(Paths.get(path))).asInstanceOf[M]

  /** Saves a protobuf message in binary format to a local file. */
  def save(path: String, model: Message): Unit = Files.write(Paths.get(path), model.toByteArray)

  /** Loads a protobuf message in text format from a local file. */
  def loadText[M <: Message: Manifest](path: String): M = loadText(Files.newInputStream(Paths.get(path)))

  /**
   * Loads a protobuf message in text format from an input stream. This is
   * useful for loading solver and model definitions from hdfs.
   * @note The stream will be closed in this function.
   */
  def loadText[M <: Message](input: InputStream)(implicit mf: Manifest[M]): M = {
    val reader = new InputStreamReader(input, UTF_8)
    val builder = mf.runtimeClass.getDeclaredMethod("newBuilder").invoke(null).asInstanceOf[Message.Builder]
    TextFormat.merge(reader, builder)
    reader.close
    input.close
    builder.build.asInstanceOf[M]
  }

  /** Saves a protobuf message in text format to a local file. */
  def saveText(path: String, model: MessageOrBuilder): Unit =
    Files.write(Paths.get(path), TextFormat.printToUnicodeString(model).getBytes(UTF_8))
}
