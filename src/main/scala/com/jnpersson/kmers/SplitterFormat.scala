/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer._
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.collection.compat.immutable.ArraySeq

/** Logic for persisting minimizer formats (ordering and parameters) to files.
 * @param P the type of MinimizerPriorities that is being managed.
 */
trait SplitterFormat[P <: MinimizerPriorities] {
  def id: String

  /**
   * Write minimizer priorities (i.e. minimizer ordering) to a file
   * @param priorities The ordering to write
   * @param props Properties that can optionally be written to
   * @param location Prefix of the location to write to. A suffix will be appended to this name.
   */
  def write(priorities: P, props: Properties, location: String)(implicit spark: SparkSession): Unit

  def read(location: String, props: Properties)(implicit spark: SparkSession): P

  def decorate(priorities: P, props: Properties): MinimizerPriorities =
    Option(props.getProperty("minimizerSpaces")) match {
      case None | Some("0") => priorities
      case Some(s) => SpacedSeed(s.toInt, priorities)
    }

  def readAndDecorate(location: String, props: Properties)(implicit spark: SparkSession): MinimizerPriorities =
    decorate(read(location, props), props)

  def readSplitter(location: String, props: Properties)(implicit spark: SparkSession): AnyMinSplitter =
    MinSplitter(readAndDecorate(location, props), props.getProperty("k").toInt)
}

/** Computed minimizer format that applies an XOR mask. */
class RandomXORFormat extends SplitterFormat[RandomXOR] {
  override def id: String = "randomXOR"

  override def read(location: String, props: Properties)(implicit spark: SparkSession): RandomXOR = {
    val mask = Option(props.getProperty("XORmask")).
      map(_.toLong).getOrElse(DEFAULT_TOGGLE_MASK)
    val m = props.getProperty("m").toInt
    val canonical = Option(props.getProperty("canonical")).getOrElse("true").toBoolean
    RandomXOR(m, mask, canonical)
  }

  /**
   * Write a minimizer ordering to a file
   *
   * @param priorities The ordering to write
   * @param props      Properties that can optionally be written to
   * @param location   Prefix of the location to write to. A suffix will be appended to this name.
   */
  override def write(priorities: RandomXOR, props: Properties, location: String)(implicit spark: SparkSession): Unit = {
    props.setProperty("XORmask", priorities.xorMask.toString)
    props.setProperty("canonical", priorities.canonical.toString)
  }
}
