/*
 * This file is part of Discount. Copyright (c) 2020 Johan Nyström-Persson.
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

package discount.hash

import discount.NTSeq

object MotifCounter {
  def apply(space: MotifSpace): MotifCounter = apply(space.byPriority.length)
  def apply(n: Int) = new MotifCounter(new Array[Int](n))

  /** Construct a MotifSpace from frequency counted motifs
   * @param counts Motifs and the number of times they were seen in the sample
   */
  def toSpaceByFrequency(counts: Array[(String, Int)]): MotifSpace = {
    new MotifSpace(
    //This must define a total ordering, otherwise a given hash can't be reliably reproduced later
      counts.sortBy(x => (x._2, x._1)).map(_._1)
    )
  }
}

/**
 * Counts all motif occurrences in a dataset to establish relative frequencies.
 * Simple in-memory counter intended for a small to medium number of distinct items indexed by integer.
 */
final case class MotifCounter(counter: Array[Int]) {

  def numMotifs: Int = counter.length

  def motifsWithCounts(space: MotifSpace): Array[(NTSeq, Int)] = space.byPriority zip counter

  def increment(motif: Motif, n: Int = 1) {
    val rank = motif.features.rank
    if (counter(rank) <= Int.MaxValue - n) {
      counter(rank) += n
    } else {
      counter(rank) = Int.MaxValue
    }
  }

  def += (motif: Motif): Unit =
    increment(motif)

  /**
   * Merge another motif counter into this one.
   * Operation only well-defined for counters based on the same motif space.
   * @param other
   */
  def += (other: MotifCounter) {
    for (i <- counter.indices) {
      val inc = other.counter(i)
      if (counter(i) <= Int.MaxValue - inc) {
        counter(i) += inc
      } else {
        counter(i) = Int.MaxValue
      }
    }
  }

  /**
   * Operation only well-defined for counters based on the same motif space.
   * To avoid allocation of potentially big arrays, we mutate this object and return it.
   * @param other
   * @return
   */
  def + (other: MotifCounter): MotifCounter = {
    this += other
    this
  }

  def sum: Long = counter.map(_.toLong).sum

  def print(space: MotifSpace, heading: String) {
    val s = sum
    def perc(x: Int) = "%.2f%%".format(x.toDouble/s * 100)

    println(heading)
    val all = motifsWithCounts(space)
    val unseen = all.filter(_._2 == 0)
    println(s"Unseen motifs: ${unseen.size}, examples: " + unseen.take(5).map(_._1).mkString(" "))
    val sorted = all.filter(_._2 > 0).sortBy(_._2)
    val rarest = sorted.take(10)
    val commonest = sorted.takeRight(10)

    val fieldWidth = space.width
    val fmt = s"%-${fieldWidth}s"
    def output(strings: Seq[String]) = strings.map(s => fmt.format(s)).mkString(" ")

    println(s"Rarest 10/${counter.size}: ")
    println(output(rarest.map(_._1)))
    println(output(rarest.map(_._2.toString)))
    println(output(rarest.map(c => perc(c._2))))

    println("Commonest 10: ")
    println(output(commonest.map(_._1)))
    println(output(commonest.map(_._2.toString)))
    println(output(commonest.map(c => perc(c._2))))
  }

  /**
   * Construct a new motif space where the least common motifs in this counter
   * have the highest priority.
   * The set of motifs will be based on the provided template.
   */
  def toSpaceByFrequency(template: MotifSpace): MotifSpace = {
    val pairs = motifsWithCounts(template)
    MotifCounter.toSpaceByFrequency(pairs)
  }
}