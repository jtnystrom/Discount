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

import com.jnpersson.kmers.input.{FileInputs, Ungrouped}
import com.jnpersson.kmers.minimizer._
import org.apache.spark.sql.SparkSession

/** Configuration parameters for the construction of a minimizer ordering for k-mers.
 *
 * @param k                 k-mer length
 * @param minimizers        source of minimizers. See [[kmers.minimizer.MinimizerSource]]
 * @param m                 minimizer width
 * @param ordering          minimizer ordering. See [[kmers.minimizer.MinimizerOrdering]]
 * @param sample            sample fraction for frequency orderings
 * @param maxSequenceLength max length of a single sequence (for short reads)
 * @param spark             the SparkSession
 */
class MinimizerConfig(k: Int, minimizers: MinimizerSource = Bundled, m: Int = 10,
                           ordering: MinimizerOrdering = Frequency, sample: Double = 0.01)
                     (implicit spark: SparkSession)  {

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
   * @return A frequency-based MinTable
   */
  private def getFrequencyTable(inFiles: List[String], validMotifs: Array[Int], width: Int,
                                persistHashLocation: Option[String] = None): MinTable = {
    val inputReader = new FileInputs(inFiles, k, Ungrouped)
    val input = inputReader.
      getInputFragments(withAmbiguous = true, Some(sample))
    sampling.createSampledTable(input, MinTable.usingRaw(validMotifs, width), sample, persistHashLocation)
  }

  private def templateTable = MinTable.ofLength(m)

  private def makeMinTableNonCanonical(ordering: MinimizerOrdering,
                                       validMotifs: Array[Int],
                                       inFiles: Option[Seq[String]],
                                       persistHash: Option[String] = None): MinTable =
    ordering match {
      case Given =>
        MinTable.usingRaw(validMotifs, m)
      case Frequency =>
        getFrequencyTable(inFiles.getOrElse(List()).toList, validMotifs, m, persistHash)
      case Lexicographic =>
        //template is lexicographically ordered by construction
        MinTable.filteredOrdering(templateTable, validMotifs)
      case XORMask(mask) =>
        //Random shuffle of a given set of minimizers
        Orderings.randomOrdering(
          MinTable.filteredOrdering(templateTable, validMotifs),
          mask
        )
      case _ => ???
    }

  /** Construct a read splitter for the given input files based on the settings in this object.
   * @param inFiles     Input files (for frequency orderings, which require sampling)
   * @param persistHash Location to persist the generated minimizer ordering (for frequency orderings), if any
   * @return a MinSplitter configured with a minimizer ordering and corresponding MinTable
   */
  def getSplitter(inFiles: Option[Seq[String]], persistHash: Option[String] = None):
  MinSplitter[_ <: MinimizerPriorities] = {

    (minimizers, ordering) match {
      case (All, XORMask(mask)) =>
        //computed RandomXOR for a wide m
        return MinSplitter(RandomXOR(m, mask, canonical = false), k)
      case (All, Canonical(XORMask(mask))) =>
        //computed RandomXOR for a wide m
        return MinSplitter(RandomXOR(m, mask, canonical = true), k)
      case _ =>
    }

    if (m > 15) {
      throw new Exception("The requested minimizer ordering can only be used with m <= 15.")
    }
    //m is now small enough to use a MinTable, which must be kept in memory

    lazy val validMotifs = minimizers.load(k, m)

    val canonicalized = ordering match {
      case Canonical(inner) =>
        val useTable = makeMinTableNonCanonical(inner, validMotifs, inFiles, persistHash)
        CanonicalPriorities.make(useTable)
      case _ =>
        makeMinTableNonCanonical(ordering, validMotifs, inFiles, persistHash)
    }

    minimizers.toSplitter(canonicalized, k)
  }
}
