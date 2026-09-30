package com.jnpersson.kmers.ry

import com.jnpersson.kmers.NTSeq
import com.jnpersson.kmers.ry.Representation1.bitToChar
import com.jnpersson.kmers.util.NTBitArray

/** Functions to support the 2 bit per character encoding of the RY
 * alphabet.
 */
object Representation2 {

  /** Convert a RY alphabet string to a 2 bit encoded NTBitArray. */
  def encode(x: String): NTBitArray = {
    val builder = NTBitArray.builder(x.length)
    var i = 0
    val len = x.length
    while (i < len) {
      builder += Representation1.charToBit(x.charAt(i))
      i += 1
    }
    builder.result
  }

  /** Convert a NTBitArray (using 2 bit per character) to a readable string,
   * using the RY alphabet.
   */
  def decode(x: NTBitArray): NTSeq = {
    val len = x.size
    val sb = new StringBuilder(len)
    var i = 0
    while (i < len) {
      val char = x(i) //fetches two bits
      sb += Representation1.bitToChar((char & 1).toByte)
      i += 1
    }
    sb.result()
  }
}
