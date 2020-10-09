package discount.hash

import discount.NTSeq

object MotifCounter {
  def apply(space: MotifSpace): MotifCounter = apply(space.byPriority.length)
  def apply(n: Int) = new MotifCounter(new Array[Int](n))

  def toSpaceByFrequency(oldSpace: MotifSpace, counts: Array[(String, Int)],
                         usedMotifs: Iterable[String]): MotifSpace = {

    val unused = counts.map(_._1).toSet -- usedMotifs
    new MotifSpace(
    //This must define a total ordering, otherwise a given hash can't be reliably reproduced later
      counts.sortBy(x => (x._2, x._1)).map(_._1),
      unused
    )
  }
}

/**
 * Counts all motif occurrences in a dataset to establish relative frequencies.
 */
final case class MotifCounter(counter: Array[Int]) {

  def numMotifs: Int = counter.length

  def motifsWithCounts(space: MotifSpace): Array[(NTSeq, Int)] = space.byPriority zip counter

  def increment(motif: Motif, n: Int = 1) {
    val rank = motif.features.tagRank
    if (counter(rank) <= Int.MaxValue - n) {
      counter(motif.features.tagRank) += n
    } else {
      counter(rank) = Int.MaxValue
    }
  }

  def += (motif: Motif): Unit = {
    increment(motif)
  }

  /**
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

    println(s"Commonest 10: ")
    println(output(commonest.map(_._1)))
    println(output(commonest.map(_._2.toString)))
    println(output(commonest.map(c => perc(c._2))))
  }

  /**
   * Construct a new motif space where the least common motifs in this counter
   * have the highest priority.
   * Other parameters (e.g. n) will be shared with the old space that this is based on.
   */
  def toSpaceByFrequency(oldSpace: MotifSpace, id: String, usedMotifs: Iterable[String]): MotifSpace = {
    val pairs = motifsWithCounts(oldSpace)
    MotifCounter.toSpaceByFrequency(oldSpace, pairs, usedMotifs)
  }

}