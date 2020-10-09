package discount.hash

import discount.NTSeq

/**
 * Looks for raw motifs in reads, counting them in a histogram.
 */
final class MotifCountingScanner(val space: MotifSpace) extends Serializable {
  @transient
  lazy val scanner = new ShiftScanner(space)

  def scanRead(counter: MotifCounter, read: NTSeq) {
    for { m <- scanner.allMatches(read) } {
      counter += m
    }
  }

  def scanGroup(counter: MotifCounter, rs: TraversableOnce[NTSeq]) {
    for (r <- rs) scanRead(counter, r)
  }
}