package com.jnpersson.discount.hash
import com.jnpersson.discount.NTSeq

import scala.collection.mutable.ArrayBuffer

object SampledFrequencies {

  /** Construct a MiniTable from frequency counted motifs
   * @param counts Motifs and the number of times they are estimated to occur in the data
   */
  def toTableByFrequency(counts: Array[(NTSeq, Long)]): MinTable = {
    val largeBuckets = counts.filter(c => c._2 >= MinSplitter.largeThreshold)

    MinTable(
      counts.map(_._1).to[ArrayBuffer],
      largeBuckets.map(_._1).to[ArrayBuffer]
    )
  }

  /** Constructs a SampledFrequencies object by in-memory counting all motifs in the input sequences,
   * using the supplied ShiftScanner. Intended for use only when Spark is not available.
   * @param scanner Scanner to use for parsing valid minimizers
   * @param inputs Input data to scan
   * @return Frequencies of all valid motifs
   */
  def fromReads(scanner: ShiftScanner, inputs: Iterator[NTSeq]): SampledFrequencies = {
    val counts = new Array[Int](scanner.priorities.numMinimizers.toInt)

    for {
      read <- inputs
      ml <- scanner.allMatches(read)._2
      m = ml.toInt
      if m != MinSplitter.INVALID
    } {
      if (counts(m) < Int.MaxValue) {
        counts(m) += 1
      } else {
        counts(m) = Int.MaxValue
      }
    }
    SampledFrequencies(scanner.priorities.asInstanceOf[MinTable], counts.indices.map(_.toLong).toArray zip counts)
  }
}

/**
 * Sampled motif frequencies that may be used to construct a new minimizer ordering.
 * @param table Template table, whose ordering of motifs will be refined based on counted frequencies.
 * @param counts Pairs of (minimizer rank, frequency).
 *               The minimizers should be a subset of those from the given template MinTable.
 */
final case class SampledFrequencies(table: MinTable, counts: Array[(Long, Int)]) {
  val lookup = new Array[Int](motifs.length)
  for { (k, v) <- counts } {
    lookup(k.toInt) = v
  }

  /** Add frequencies from the other object to this one */
  def add(other: SampledFrequencies): SampledFrequencies = {
    val r = other.counts.map(x => (x._1, x._2 + lookup(x._1.toInt)))
    SampledFrequencies(table, r)
  }

  private def motifs = table.byPriority

  /** A sorted array of all motifs in the template space, refined based on the observed frequencies.
   * Defines a minimizer ordering.
   * @return Pairs of (motif, frequency)
   */
  lazy val motifsWithCounts: Array[(NTSeq, Int)] = {
    val r = motifs.iterator.zipWithIndex.map(x => (x._1, lookup(x._2))).toArray
    //Construct the minimizer ordering. The sort is stable and respects the ordering in the template space,
    //so equally frequent motifs will remain in the old order relative to each other.
    val ord: Ordering[(NTSeq, Int)] = Ordering.by(x => x._2)
    java.util.Arrays.sort(r, ord)
    r
  }


  /** Print a summary of what has been counted, including the most and least frequent motifs */
  def print(): Unit = {
    val sum = counts.map(_._2.toLong).sum

    def perc(x: Int) = "%.2f%%".format(x.toDouble/sum * 100)

    val all = motifsWithCounts
    val (unseen, seen) = all.partition(_._2 == 0)
    println(s"Unseen motifs: ${unseen.length}, examples: " + unseen.take(5).map(_._1).mkString(" "))

    val rarest = seen.take(10)
    val commonest = seen.takeRight(10)

    val fieldWidth = table.width
    val fmt = s"%-${fieldWidth}s"
    def output(strings: Seq[String]) = strings.map(s => fmt.format(s)).mkString(" ")

    println(s"Rarest 10/${counts.length}: ")
    println(output(rarest.map(_._1)))
    println(output(rarest.map(_._2.toString)))
    println(output(rarest.map(c => perc(c._2))))

    println("Commonest 10: ")
    println(output(commonest.map(_._1)))
    println(output(commonest.map(_._2.toString)))
    println(output(commonest.map(c => perc(c._2))))
  }

  /**
   * Construct a new MinTable (minimizer ordering) where the least common motifs in this counter
   * have the highest priority.
   */
  def toTable(sampledFraction: Double): MinTable = {
    if (!lookup.exists(_ > 0)) {
      println("Warning: no motifs were counted, so the motif frequency distribution will be unreliable.")
      println("Try increasing the sample fraction (--sample). For very small datasets, this warning may be ignored.")
    }

    val perMotifCounts = motifsWithCounts.map(x => (x._1, (x._2.toLong / sampledFraction).toLong))
    SampledFrequencies.toTableByFrequency(perMotifCounts)
  }
}