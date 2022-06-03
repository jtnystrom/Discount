package com.jnpersson.discount.hash
import com.jnpersson.discount.NTSeq

object SampledFrequencies {

  /** Construct a MotifSpace from frequency counted motifs
   * @param counts Motifs and the number of times they are estimated to occur in the data
   */
  def toSpaceByFrequency(counts: Array[(NTSeq, Long)]): MotifSpace = {
    val largeBuckets = counts.filter(c => c._2 >= MinSplitter.largeThreshold)

    new MotifSpace(
      counts.map(_._1),
      largeBuckets.map(_._1)
    )
  }

  /** Constructs a SampledFrequencies object by in-memory counting all motifs in the input sequences,
   * using the supplied ShiftScanner. Intended for use only when Spark is not available.
   * @param scanner Scanner to use for parsing valid minimizers
   * @param inputs Input data to scan
   * @return Frequencies of all valid motifs
   */
  def fromReads(scanner: ShiftScanner, inputs: Iterator[NTSeq]): SampledFrequencies = {
    val counts = new Array[Int](scanner.space.byPriority.length)

    for {
      read <- inputs
      m <- scanner.allMatches(read)._2
      if m != MinSplitter.INVALID
    } {
      if (counts(m) < Int.MaxValue) {
        counts(m) += 1
      } else {
        counts(m) = Int.MaxValue
      }
    }
    SampledFrequencies(scanner.space, counts.zipWithIndex.map(x => (x._2, x._1)))
  }
}

/**
 * Sampled motif frequencies that may be used to construct a new minimizer ordering.
 * @param space Template MotifSpace, whose ordering of motifs will be refined based on counted frequencies.
 * @param counts Pairs of (minimizer rank, frequency).
 *               The minimizers should be a subset of those from the given template MotifSpace.
 */
final case class SampledFrequencies(space: MotifSpace, counts: Array[(Int, Int)]) {
  val lookup = new Array[Int](motifs.length)
  for { (k, v) <- counts } {
    lookup(k) = v
  }

  private def motifs = space.byPriority

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

    val fieldWidth = space.width
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
   * Construct a new motif space (minimizer ordering) where the least common motifs in this counter
   * have the highest priority.
   */
  def toSpace(sampledFraction: Double): MotifSpace = {
    if (!lookup.exists(_ > 0)) {
      println("Warning: no motifs were counted, so the motif frequency distribution will be unreliable.")
      println("Try increasing the sample fraction (--sample). For very small datasets, this warning may be ignored.")
    }

    val perMotifCounts = motifsWithCounts.map(x => (x._1, (x._2.toLong / sampledFraction).toLong))
    SampledFrequencies.toSpaceByFrequency(perMotifCounts)
  }
}