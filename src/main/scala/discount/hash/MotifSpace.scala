package discount.hash

import discount.NTSeq
import discount.util.BPBuffer

import scala.collection.Seq

object MotifSpace {
  val all1mersDNA = Seq("A", "C", "G", "T")
  val all1mersRNA = Seq("A", "C", "G", "U")

  /**
   * Generate all sequences of the given length in lexicographic order.
   * @param length
   * @param rna
   * @return
   */
  def motifsOfLength(length: Int, rna: Boolean = false): Seq[String] = {
    val bases = if (rna) all1mersRNA else all1mersDNA
    if (length == 1) {
      bases
    } else if (length > 1) {
      motifsOfLength(length - 1, rna).flatMap(x => bases.iterator.map(y => x + y))
    } else {
      throw new Exception(s"Unsupported motif length $length")
    }
  }

  def ofLength(w: Int, rna: Boolean): MotifSpace = using(motifsOfLength(w, rna))

  def using(mers: Seq[String]) = new MotifSpace(mers.toArray)
}

/**
 * A set of motifs that can be used, and their relative priorities.
 * @param n Number of motifs in a motif set.
 * @param unusedMotifs Set of motifs that are not to be used (ignored if encountered)
 */
final case class MotifSpace(byPriority: Array[NTSeq],
                            unusedMotifs: Set[NTSeq] = Set.empty) {
  val width = byPriority.map(_.length()).max
  def maxMotifLength = width
  val minMotifLength = byPriority.map(_.length()).min

  @volatile
  private var lookup = Map.empty[NTSeq, Features]

  def getFeatures(pattern: NTSeq): Features = {
    if (!lookup.contains(pattern)) {
      synchronized {
        if (!lookup.contains(pattern)) {
          val f = new Features(pattern, priorityOf(pattern), true)
          lookup += pattern -> f
        }
      }
    }
    lookup(pattern)
  }

  def get(pattern: NTSeq, pos: Int): Motif = {
    Motif(pos, getFeatures(pattern))
  }

  def create(pattern: NTSeq, pos: Int): Motif = {
    Motif(pos, new Features(pattern, priorityOf(pattern), true))
  }

  //4 ^ width
  val maxMotifs = 4 << (width * 2 - 2)

  //bit shift distance
  val shift = 32 - (width * 2)

  /**
   * Compute lookup index for a motif. Inefficient, not for frequent use.
   * Only works for widths up to 15 (30 bits).
   * Note: this mechanism, and the ones using it below, will not support mixed-length motifs.
   * @param m
   * @return
   */
  def motifToInt(m: NTSeq) = {
    val wrapped = BPBuffer.wrap(m)
    BPBuffer.computeIntArrayElement(wrapped.data, 0, width, 0) >>> shift
  }

  val priorityLookup = new Array[Int](maxMotifs)
  for ((motif, pri) <- byPriority.iterator.zipWithIndex) {
    priorityLookup(motifToInt(motif)) = pri
  }

  def priorityOf(mk: NTSeq) =
    priorityLookup(motifToInt(mk))

  val compactBytesPerMotif = if (width > 8) 4 else if (width > 4) 2 else 1
}