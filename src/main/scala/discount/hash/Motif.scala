package discount.hash

import discount.NTSeq
import discount.hash.PosRankWindow._
import discount.hash.Motif._

import scala.language.postfixOps

object Motif {
  val Empty = Motif(0, Features("", 0, false))

  /**
   * The attributes of a motif, except its position.
   * @param rank priority/unique ID of this motif. Lower value indicates higher priority.
   */
  final case class Features(pattern: NTSeq, rank: Int, valid: Boolean)
}

/** A single motif (a potential minimizer) in a nucleotide sequence.
 * @param pos
 * @param features
 */
final case class Motif(pos: Int, features: Features) extends MotifContainer {
  /** The nucleotide sequence */
  def pattern: NTSeq = features.pattern

  override def toString = "[%s,%02d]".format(pattern, pos)

  def motif = this
}
