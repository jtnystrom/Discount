/*
 * This file is part of Discount. Copyright (c) 2019-2026 Johan Nyström-Persson.
 *
 *  Discount is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Discount is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer.{MinSplitter, MinimizerPriorities}

/** Management of the set of minimizer formats supported by an application.
 * Allows for constructing a MinSplitter from a configuration, and for looking up SplitterFormat by
 * id (string) or by class.
 * @tparam C Supported configuration type
 */
trait MinimizerFormats[C <: MinimizerCLIConf] {
  protected def formatsById: Map[String, SplitterFormat[_]]
  protected def formatsByCls: Map[Class[_], SplitterFormat[_]]

  /** Make a MinSplitter from a configuration. */
  def makeSplitter(config: C): MinSplitter[_]

  /** Obtain a previously registered SplitterFormat by id */
  def getFormat(id: String): SplitterFormat[_] = synchronized {
    formatsById.getOrElse(id, throw new Exception(s"No such format $id"))
  }

  /** Obtain a previously registered SplitterFormat by class */
  def getFormat[P <: MinimizerPriorities](cls: Class[_ <: P]): SplitterFormat[P] = synchronized {
    formatsByCls.getOrElse(cls, throw new Exception(s"No format for class $cls")).asInstanceOf[SplitterFormat[P]]
  }

  def getFormat[P <: MinimizerPriorities](p: P): SplitterFormat[P] =
    getFormat(p.getClass)
}

