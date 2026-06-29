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

package com.jnpersson.kmers.input

import com.jnpersson.kmers.SeqTitle
import com.jnpersson.kmers.minimizer.InputFragment
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.lit

/**
 * Routines for parsing input from a DataFrame directly.
 */
object DirectInputs {

  /** Read inputs directly from a DataFrame.
   * The DataFrame must have the following columns:
   * header, nucleotides, (optionally) nucleotides2
   */
  def forDataFrame(data: DataFrame)(implicit spark: SparkSession) : InputReader =
    new DirectInputReader(data)

  /** Read paired-end reads directly from DataFrames.
   * The DataFrames must have the following columns:
   * header, nucleotides, (optionally) nucleotides2
   */
  def forPairs(data1: DataFrame, data2: DataFrame)(implicit spark: SparkSession) : PairedInputReader =
    new PairedInputReader(forDataFrame(data1), forDataFrame(data2))

}

/** A reader that converts a DataFrame directly into a dataset of [[InputFragment]]. */
class DirectInputReader(data: DataFrame)(implicit spark: SparkSession) extends InputReader {
  import spark.sqlContext.implicits._

  /**
   * Sequence titles in this file
   *
   * @return
   */
  override def getSequenceTitles: Dataset[SeqTitle] =
    data.select("header").as[SeqTitle]

  /**
   * Read sequence data as fragments from the input files, removing any newlines.
   * @return
   */
  override protected[input] def getFragments(): Dataset[InputFragment] = {
    val hasN2 = data.columns.contains("nucleotides2")
    val hasLocation = data.columns.contains("location")
    data.select($"header",
      if (hasLocation) $"location" else lit(1L).as("location"),
      $"nucleotides",
      if (hasN2) $"nucleotides2" else lit(null).as("nucleotides2")
    ).as[InputFragment]
  }
}