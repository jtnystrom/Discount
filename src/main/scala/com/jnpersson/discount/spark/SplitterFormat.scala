/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
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

package com.jnpersson.discount.spark

import com.jnpersson.discount.hash.{MinSplitter, MinTable, MinimizerPriorities}
import org.apache.spark.sql.SparkSession

import java.util.Properties

trait SplitterFormat[P <: MinimizerPriorities] {
  def id: String

  /**
   * Write a MinSplitter　(e.g. minimizer ordering) to a file
   * @param table The ordering to write
   * @param props Properties that can optionally be written to
   * @param location Prefix of the location to write to. A suffix will be appended to this name.
   */
  def write(splitter: MinSplitter[P], props: Properties, location: String)(implicit spark: SparkSession): Unit

  def read(location: String, props: Properties)(implicit spark: SparkSession): MinSplitter[P]

}

class StandardFormat extends SplitterFormat[MinTable] {
  val id = "standard"

  /**
   * Write a MinTable's minimizer ordering to a file
   * @param table The ordering to write
   * @param location Prefix of the location to write to. A suffix will be appended to this name.
   */
  def write(splitter: MinSplitter[MinTable], props: Properties, location: String)(implicit spark: SparkSession): Unit = {
    val persistLoc = s"${location}_minimizers.txt"
    val table = splitter.priorities
    HDFSUtil.writeTextFile(persistLoc, table.byPriority.mkString("", "\n", "\n"))
    println(s"Saved ${table.byPriority.length} minimizers to $persistLoc")
  }

  def read(location: String, props: Properties)(implicit spark: SparkSession): MinSplitter[MinTable] = {
    val k = props.getProperty("k").toInt
    val minLoc = s"${location}_minimizers.txt"
    val use = (new Sampling).readMotifList(minLoc)
    println(s"${use.length} motifs will be used (loaded from $minLoc)")
    MinSplitter(MinTable.using(use), k)
  }
}
