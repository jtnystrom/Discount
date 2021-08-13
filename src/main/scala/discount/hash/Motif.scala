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
  final case class Features(pattern: NTSeq, rank: Int, valid: Boolean) {

    def equivalent(other: Features) = {
      //rank is sufficient to identify pattern
      rank == other.rank
    }
  }
}

/** A single motif (a potential minimizer) in a nucleotide sequence.
 * @param pos
 * @param features
 */
final case class Motif(pos: Int, features: Features) extends MotifContainer {
  /** The nucleotide sequence */
  def pattern = features.pattern

  //Note: implicit assumption that pos < 100 when we use this
  lazy val packedString = {
    val r = new StringBuilder
    packInto(r)
    r.toString()
  }

  def packInto(r: StringBuilder) {
    r.append(pattern)
    if (pos < 10) {
      r.append("0")
    }
    r.append(pos)
  }

  override def toString = "[%s,%02d]".format(pattern, pos)

  def fastEquivalent(other: Motif) = {
    pos == other.pos && ((features eq other.features) || features.equivalent(other.features))
  }

  override def equals(other: Any) = {
    other match {
      case m: Motif => this.fastEquivalent(m)
      case _ => false
    }
  }

  override lazy val hashCode: Int = pos.hashCode * 41 + features.hashCode

  def motif = this
}
