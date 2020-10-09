package discount.hash
import discount.NTSeq

import scala.language.postfixOps

/**
 * The attributes of a motif, except its position.
 */
final case class Features(val tag: NTSeq, val tagRank: Int, valid: Boolean)

final case class Motif(pos: Int, features: Features) extends MotifContainer {
  def tag = features.tag

  override def toString = "[%s,%02d]".format(tag, pos)

  def motif = this
}
