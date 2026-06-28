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

import org.apache.spark.sql.functions.{array, col, element_at}
import org.apache.spark.sql.{Column, SparkSession}

/** Methods to support an index of key-value pairs
 * with a fixed number of id columns (Longs) that contain an encoded k-mer.
 */

trait KmerKeyedIndex {
  def idLongs: Int

  val spark: SparkSession
  import spark.sqlContext.implicits._

  def idColumnsTypes: String = idColumnNames.map(c => s"$c long").mkString(",")

  /** All key columns as a single string */
  def idColumnsString: String = idColumnNames.mkString(",")

  /** All key columns */
  def idColumns: Seq[Column] = idColumnNames.map(col)

  def numIdColumns: Int = idColumns.size

  /** Names of the key columns */
  def idColumnNames: Seq[String] =
    (1 to idLongs).map(i => s"id$i")

  /** Unpack the minimizer array column to multiple columns */
  def idColumnsFromMinimizer: Seq[Column] =
    (1 to idLongs).map(i => element_at($"minimizer", i).as(s"id$i"))

  /** Pack the minimizer columns into a single array */
  def minimizerColumnFromIdColumns: Column =
    array(idColumns :_*).as("minimizer")

}

