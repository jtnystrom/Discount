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

package com.jnpersson.discount.hash
import com.jnpersson.discount.NTSeq
import com.jnpersson.discount.spark.Output
import com.jnpersson.discount.util.{Arrays, NTBitArray}
import it.unimi.dsi.fastutil.ints.{IntArrays, IntComparator}

object SampledFrequencies {

  /** Constructs a SampledFrequencies object by in-memory counting all motifs in the input sequences,
   * using the supplied ShiftScanner. Intended for use only when Spark is not available.
   * @param scanner Scanner to use for parsing valid minimizers
   * @param inputs Input data to scan
   * @return Frequencies of all valid motifs
   */
  def fromReads(scanner: ShiftScanner, inputs: Iterator[NTSeq]): SampledFrequencies = {
    val counts = scanner.priorities.numMinimizers match {
      case None =>
        throw new Exception("Sampling based on these minimizer priorities is not possible: numMinimizers is undefined.")
      case Some(n) =>
        assert(n < Int.MaxValue)
        new Array[Int](n.toInt)
    }

    for {
      read <- inputs
      ml <- scanner.allMatches(read)._2.validBitArrayIterator
      m = ml.toInt
    } {
      if (counts(m) < Int.MaxValue) {
        counts(m) += 1
      } else {
        counts(m) = Int.MaxValue
      }
    }
    apply(scanner.priorities.asInstanceOf[MinTable],
      (Iterator.range(0, counts.length) zip counts.iterator))
  }

  /**
   * Sampled motif frequencies that may be used to construct a new minimizer ordering.
   * @param table Template table, whose ordering of motifs will be refined based on counted frequencies.
   * @param counts Pairs of (minimizer rank, frequency).
   *               The minimizers should be a subset of those from the given template MinTable.
   */
  def apply(table: MinTable, counts: Iterator[(Int, Int)]): SampledFrequencies = {
    //Constructing the lookup array is done in a separate method here to let it be GC'ed after SampledFrequencies
    //has been constructed, reducing memory pressure.
    val lookup = new Array[Int](Arrays.max(table.byPriority) + 1)
    for { (k, v) <- counts } {
      //mapping motif to count
      lookup(table.byPriority(k)) = v
    }
    SampledFrequencies(table, lookup)
  }
}

/**
 * Sampled motif frequencies that may be used to construct a new minimizer ordering.
 * @param table Template table, whose ordering of motifs will be refined based on counted frequencies.
 *              This table will be mutated and cannot be reused after passing into SampledFrequencies.
 * @param minimizerCounts Maps encoded minimizer to count.
 */
final case class SampledFrequencies(table: MinTable, minimizerCounts: Array[Int]) {
  private def width = table.width
  private def motifs: Array[Int] = table.byPriority

  /** A sorted array of all motifs in the template space, based on the observed frequencies.
   * Defines a minimizer ordering.
   * @return Pairs of (motif, frequency)
   */
  lazy val sortedMotifs: Array[Int] = {
    //Construct the minimizer ordering. The sort is stable and respects the ordering in the template space,
    //so equally frequent motifs will remain in the old order relative to each other.

    val r = table.byPriority
    //Using the fastutils sort rather than scala array sort to avoid boxing of integers for this case
    IntArrays.parallelQuickSort(r, new IntComparator {
      override def compare(k1: Int, k2: Int): Int = Integer.compare(minimizerCounts(k1), minimizerCounts(k2))
    })
    r
  }

  def motifsAndCounts: Iterator[(Int, Int)] =
    sortedMotifs.iterator.map(x => (x, minimizerCounts(x)))

  private def countUnseen(): Int = {
    var r = 0
    var i = 0
    while (i < motifs.length) {
      if (minimizerCounts(motifs(i)) == 0) r += 1
        i += 1

    }
    r
  }

  /** Print a summary of what has been counted, including the most and least frequent motifs */
  def print(): Unit = {
    val sum = Arrays.sum(minimizerCounts)
    val unseenCount = countUnseen()

    def percent(x: Int) = Output.formatPerc(x.toDouble/sum)

    def ntString(p: Int) = NTBitArray.fromLong(p, width).toString

    val rarest = sortedMotifs.iterator.filter(minimizerCounts(_) > 0).take(10).toSeq
    val commonest = sortedMotifs.takeRight(10)

    val fieldWidth = table.width
    val fmt = s"%-${fieldWidth}s"
    def output(strings: Seq[String]) = strings.map(s => fmt.format(s)).mkString(" ")

    println(s"Unseen motifs: ${unseenCount}")
    println(s"Rarest 10/${motifs.length}: ")
    println(output(rarest.map(p => ntString(p))))
    println(output(rarest.map(p => minimizerCounts(p).toString)))
    println(output(rarest.map(p => percent(minimizerCounts(p)))))

    println("Commonest 10: ")
    println(output(commonest.map(p => ntString(p))))
    println(output(commonest.map(p => minimizerCounts(p).toString)))
    println(output(commonest.map(p => percent(minimizerCounts(p)))))
  }

  /**
   * Construct a new MinTable (minimizer ordering) where the least common motifs in this counter
   * have the highest priority.
   */
  def toTable(): MinTable = {
    if (!minimizerCounts.exists(_ > 0)) {
      println("Warning: no motifs were counted, so the motif frequency distribution will be unreliable.")
      println("Try increasing the sample fraction (--sample). For very small datasets, this warning may be ignored.")
    }

    val numLargeBuckets = minimizerCounts.count(_ >= MinSplitter.largeThreshold)
    MinTable(sortedMotifs, width, numLargeBuckets)
  }
}