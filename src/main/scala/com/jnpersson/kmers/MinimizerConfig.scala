/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer._
import org.apache.spark.sql.SparkSession

/** Configuration parameters for the construction of a minimizer ordering for k-mers.
 *
 * @param k                 k-mer length
 * @param minimizers        source of minimizers. See [[MinimizerSource]]
 * @param m                 minimizer width
 * @param ordering          minimizer ordering. See [[MinimizerOrdering]]
 * @param sample            sample fraction for frequency orderings
 * @param maxSequenceLength max length of a single sequence (for short reads)
 * @param spark             the SparkSession
 */
class MinimizerConfig(k: Int, minimizers: MinimizerSource = Bundled, m: Int = 10,
                           ordering: MinimizerOrdering = Frequency(), sample: Double = 0.01,
                           maxSequenceLength: Int = 1000000)(implicit spark: SparkSession)  {

  //Validate configuration
  if (m > k) {
    throw new Exception("m must be <= k")
  }

  private def sampling = new Sampling

  /** Efficient frequency MinTable construction method.
   * The ordering of validMotifs will be preserved in the case of equally frequent motifs.
   * @param inFiles files to sample
   * @param validMotifs valid minimizers to keep (others will be ignored)
   * @param persistHashLocation location to persist the generated minimizer ordering, if any
   * @param bySequence count the number of distinct sequences that motifs appear in, instead of the aggregate count
   * @return A frequency-based MinTable
   */
  private def getFrequencyTable(inFiles: List[String], validMotifs: Array[Int], width: Int,
                                persistHashLocation: Option[String] = None,
                                bySequence: Boolean = false): MinTable = {
    val inputReader = new Inputs(inFiles, k, maxSequenceLength, false)
    val input = inputReader.
      getInputFragments(false, withAmbiguous = true, Some(sample))
    sampling.createSampledTable(input, MinTable.usingRaw(validMotifs, width), sample, persistHashLocation, bySequence)
  }

  private def templateTable = MinTable.ofLength(m)

  /** Construct a read splitter for the given input files based on the settings in this object.
   * @param inFiles     Input files (for frequency orderings, which require sampling)
   * @param persistHash Location to persist the generated minimizer ordering (for frequency orderings), if any
   * @return a MinSplitter configured with a minimizer ordering and corresponding MinTable
   */
  def getSplitter(inFiles: Option[Seq[String]], persistHash: Option[String] = None):
  MinSplitter[_ <: MinimizerPriorities] = {

    (minimizers, ordering) match {
      case (All, XORMask(mask, canonical)) =>
        //computed RandomXOR for a wide m
        return MinSplitter(RandomXOR(m, mask, canonical = canonical), k)
      case _ =>
    }

    if (m > 15) {
      throw new Exception("The requested minimizer ordering can only be used with m <= 15.")
    }
    //m is now small enough to use a MinTable, which must be kept in memory

    lazy val validMotifs = minimizers.load(k, m)

    val useTable = ordering match {
      case Given => MinTable.usingRaw(validMotifs, m)
      case Frequency(bySequence) =>
        getFrequencyTable(inFiles.getOrElse(List()).toList, validMotifs, m, persistHash, bySequence)
      case Lexicographic =>
        //template is lexicographically ordered by construction
        MinTable.filteredOrdering(templateTable, validMotifs)
      case XORMask(mask, canonical) =>
        //Random shuffle of a given set of minimizers
        //canonical is ignored here.
        Orderings.randomOrdering(
          MinTable.filteredOrdering(templateTable, validMotifs),
          mask
        )
      case Signature =>
        Orderings.minimizerSignatureTable(templateTable)
    }

    minimizers.toSplitter(useTable, k)
  }
}
