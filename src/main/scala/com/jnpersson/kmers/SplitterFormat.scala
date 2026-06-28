/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer._
import org.apache.spark.sql.SparkSession

import java.util.Properties

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

  def decorate(priorities: P, props: Properties)(implicit spark: SparkSession): MinimizerPriorities = {
    Option(props.getProperty("minimizerSpaces")) match {
      case None | Some("0") => priorities
      case Some(s) => SpacedSeed(s.toInt, priorities)
    }
  }

  def readAndDecorate(location: String, props: Properties)(implicit spark: SparkSession): MinimizerPriorities =
    decorate(read(location, props), props)

  def readSplitter(location: String, props: Properties)(implicit spark: SparkSession): AnyMinSplitter =
    MinSplitter(readAndDecorate(location, props), props.getProperty("k").toInt)
}

class StandardFormat extends SplitterFormat[MinTable] {
  val id = "standard"

  /**
   * Write a MinTable's minimizer ordering to a file
   * @param table The ordering to write
   * @param location Prefix of the location to write to. A suffix will be appended to this name.
   */
  def write(table: MinTable, props: Properties, location: String)(implicit spark: SparkSession): Unit = {
    val persistLoc = s"${location}_minimizers.txt"
    HDFSUtil.writeTextLines(persistLoc, table.humanReadableIterator)
    println(s"Saved ${table.byPriority.length} minimizers to $persistLoc")
  }

  def read(location: String, props: Properties)(implicit spark: SparkSession): MinTable = {
    val minLoc = s"${location}_minimizers.txt"
    val s = new Sampling
    val use = s.readMotifList(minLoc).collect()
    println(s"${use.length} motifs will be used (loaded from $minLoc)")
    val tableWidth = s.readMotifWidth(minLoc)
    MinTable.usingRaw(use, tableWidth)
  }
}

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
