package com.jnpersson.discount.hash

import com.jnpersson.discount.NTSeq

import scala.language.postfixOps

/**
 * The attributes of a motif, except its position.
 * @param rank priority/unique ID of this motif. Lower value indicates higher priority.
 */
final case class Features(pattern: NTSeq, rank: Int, valid: Boolean)

object Motif {
  val Empty = Motif(0, Features("", 0, false))

  val INVALID = -1
}

/**
 * A motif from a nucleotide sequence.
 * @param pos
 * @param features
 */
final case class Motif(pos: Int, features: Features) {
  /** The nucleotide sequence */
  def pattern = features.pattern

  override def toString = "[%s,%02d]".format(pattern, pos)

  def motif = this
}
