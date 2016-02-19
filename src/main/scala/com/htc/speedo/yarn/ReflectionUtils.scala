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

package com.htc.speedo.yarn

import scala.reflect.runtime.universe._
import scala.util.Try

import com.twitter.scalding.Args

/**
 * Reflection utilities using scala reflect library.
 *
 * @author Zhongyang Zheng (zhongyang_zheng@htc.com)
 */
object ReflectionUtils {
  /** This is required for reflection */
  lazy val mirror = runtimeMirror(getClass.getClassLoader)

  /**
   * Get companion object for a class. The class can be class of companion object or the class itself.
   * @example Suppose we have {{{
   * class A
   * object A
   * }}}
   * both `companionObjectOption[A]` and `companionObjectOption(A.getClass)` returns the companion
   * object `A`. Returns None for classes without companion objects (including java classes).
   */
  def companionObjectOption(clazz: Class[_]): Option[Any] =
    Try(mirror.reflectModule(mirror.moduleSymbol(clazz)).instance).toOption

  /**
   * Get all constructors of the given class match the given condition based on
   * parameter types.
   * @return A list of constructors. Each constructor is a tuple, the first element is the method
   * mirror, which can be used to invoke the constructor using apply method; the second element is
   * a list of parameter class names.
   * @note The class names may not exist if they are generic.
   */
  def getConstructors(clazz: Class[_], filter: List[String] => Boolean): List[(MethodMirror, List[String])] = {
    val symbol = mirror.classSymbol(clazz) // the class symbol
    val classMirror = mirror.reflectClass(symbol) // the class mirror
    if (!symbol.isModuleClass && !symbol.isAbstractClass)
      symbol.toType.members.toList
        .collect {
          case m: MethodSymbol if m.isConstructor && m.owner == symbol =>
            (m, m.paramss.flatten.map(_.typeSignature.typeSymbol.fullName))
        }
        .collect { case (m, param) if filter(param) => (classMirror.reflectConstructor(m), param) }
    else Nil // either object only or class or trait
  }

  /**
   * Get an instance of an object or create a class with default constructor from object/class name.
   */
  def getInstaceFrom(fullName: String, args: Args): Option[Any] =
    getInstaceFrom(Class.forName(fullName), args)

  /**
   * Get an instance frm given java class. The instance tries to:
   *  1. Create instance with constructor with scalding [[Args]] parameter
   *  2. Create instance with constructor with no parameters
   *  3. Companion object
   *  4. Return None
   */
  def getInstaceFrom(clazz: Class[_], args: Args): Option[Any] = {
    // full class name for scalding Args
    val argName = classOf[Args].getName
    // try to get constructors with 0 parameters or 1 Args parameter
    getConstructors(clazz, params => params == Nil || params == List(argName)) match {
      case Nil => companionObjectOption(clazz) // try companion object
      case list: List[(MethodMirror, List[String])] =>
        val (m, param) = list.maxBy(_._2.size) // use constructor with most args
        Some(m(param.map(_ => args): _*))
    }
  }
}
