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

package org.specs2

import akka.testkit.TestKitBase

import org.specs2.specification.core.mutable.SpecificationStructure
import org.specs2.specification.create.SpecificationCreation
import org.specs2.specification.dsl.mutable.MutableDsl
import org.specs2.specification.mutable.SpecificationFeatures

/**
 * A base class for using specs2 specification with akka test kit.
 * This depends on internal class SpecificationStructure, and must be placed in org.specs2 package.
 */
abstract class AkkaSpecification extends TestKitBase with SpecificationStructure
  with SpecificationFeatures with SpecificationCreation with MutableDsl
