/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.discount.util

import com.jnpersson.discount.{Both, NTSeq, Orientation}
import BitRepresentation._
import com.jnpersson.discount.util.KmerTable.BuildParams

import java.nio.ByteBuffer
import scala.annotation.tailrec

/** Methods for decoding NT sequences of a fixed max length, with reusable buffers. */
class NTBitDecoder(buffer: ByteBuffer, builder: StringBuilder) {

  def bytesToString(bytes: Array[Byte], offset: Int, size: Int): NTSeq = {
    builder.clear()
    BitRepresentation.bytesToString(bytes, builder, offset, size)
  }

  /**
   * Decode a previously encoded NT sequence to human-readable string form.
   *
   * @param data encoded data
   * @param offset 0-based offset in the data array to start from
   * @param size number of letters to decode
   * @return decoded string
   */
  def longsToString(data: Array[Long], offset: Int, size: Int): NTSeq = {
    buffer.clear()
    var i = 0
    while (i < data.length) {
      buffer.putLong(data(i))
      i += 1
    }
    bytesToString(buffer.array(), offset, size)
  }

  /**
   * Decode a previously encoded NT sequence to human-readable string form.
   *
   * @param data encoded data
   * @param size number of letters to decode, starting from offset 0 (leftmost)
   * @return decoded string
   */
  def longToString(data: Long, size: Int): NTSeq =
    longsToString(Array(data), 0, size)

  def toString(ar: NTBitArray): NTSeq =
    longsToString(ar.data, 0, ar.size)

}

object NTBitArray {

  /** Reversibly encode a nucleotide sequence as an array of 64-bit longs.
   * The 2*length leftmost bits in the array will be populated.
   */
  def encode(data: NTSeq): NTBitArray = {
    val buf = longBuffer(data.length)
    var longIdx = 0
    var qs = 0
    while (longIdx < buf.length) {
      var quadIdx = 0
      var qshift = 56
      var x = 0L
      while (quadIdx < 8 && qs < data.length) {
        val q = quadToByte(data, qs)
        x = x | ((q.toLong & 255L) << qshift)
        qs += 4
        qshift -= 8
        quadIdx += 1
      }
      buf(longIdx) = x
      longIdx += 1
    }
    NTBitArray(buf, data.length)
  }

  /** Fill an NTBitArray with all zeroes (the nucleotide A) */
  def blank(size: Int): NTBitArray =
    NTBitArray(longBuffer(size), size)

  /** Populate an NTBitArray with a fixed pattern repeatedly to reach a given size. No shifting will be performed. */
  def fill(data: Long, size: Int): NTBitArray = {
    val r = longBuffer(size, data)
    if (size % 32 != 0) {
      val finalMask = -1L << (64 - (size % 32) * 2)
      r(r.length - 1) = r(r.length - 1) & finalMask
    }
    NTBitArray(r, size)
  }

  /** Populate a new NTBitArray with a single right-aligned long, shifting it to be left-aligned.
   * Only well defined when the data fits inside a positive long, i.e. size <= 31.
   * The inverse mapping is [[NTBitArray.toLong]]. */
  def fromLong(data: Long, size: Int): NTBitArray = {
    val shift = (32 - size) * 2
    NTBitArray(Array(data << shift), size)
  }

  def longsForSize(size: Int): Int =
    if (size % 32 == 0) { size >> 5 } else { (size >> 5) + 1 }

  def longBuffer(size: Int): Array[Long] =
    new Array[Long](longsForSize(size))

  def longBuffer(size: Int, fill: Long): Array[Long] =
    Arrays.fillNew(longsForSize(size), fill)


  /** Shift an array of two-bits one step to the left, dropping one bp, and inserting another on the right.
   *
   * @param data     The sequence to shift
   * @param addRight New two-bit nucleotide to insert on the right
   * @param k        k
   */
  def shiftLongArrayKmerLeft(data: Array[Long], addRight: Byte, k: Int): Unit = {
    val n = data.length
    var i = 0
    while (i < n - 1) {
      data(i) = (data(i) << 2) | (data(i + 1) >>> 62)
      i += 1
    }
    //i == n -1
    val kmod32 = k & 31
    data(i) = (data(i) << 2) | (addRight.toLong << ((32 - kmod32) * 2))
  }

  /** Shift an array of two-bits one step to the left, dropping one bp, and inserting another on the right.
   * Write the result to a KmerTableBuilder.
   *
   * @param data        The sequence to shift
   * @param addRight    New two-bit nucleotide to insert on the right
   * @param k           k
   * @param destination KmerTableBuilder where the result should be inserted
   */
  def shiftLongKmerAndWrite(data: Array[Long], addRight: Byte, k: Int, destination: KmerTableBuilder): Unit = {
    val n = data.length
    var i = 0
    while (i < n - 1) {
      val x = (data(i) << 2) | (data(i + 1) >>> 62)
      data(i) = x
      destination.addLong(x)
      i += 1
    }
    //i == n -1
    val kmod32 = k & 31
    val x = (data(i) << 2) | (addRight.toLong << ((32 - kmod32) * 2))
    data(i) = x
    destination.addLong(x)
  }

  /**
   * A decoder that can decode NT sequences of a fixed max length.
   */
  def fixedSizeDecoder(size: Int): NTBitDecoder = {
    val sb = new StringBuilder
    val bytes = if (size % 32 == 0) size / 4 else size / 4 + 8
    val buf = ByteBuffer.allocate(bytes)
    new NTBitDecoder(buf, sb)
  }

  /**
   * Decode a previously encoded NT sequence to human-readable string form.
   *
   * @param data   encoded data
   * @param offset 0-based offset in the data array to start from
   * @param size   number of letters to decode
   * @return decoded string
   */
  def longsToString(data: Array[Long], offset: Int, size: Int): NTSeq =
    fixedSizeDecoder(size).longsToString(data, offset, size)
}

/**
 * A bit-packed sequence of nucleotides, where each letter is represented by two bits.
 * Some operations mutate this object, and others return a new object instead.
 * A new copy can always be obtained with clone(). No operations can change the size of the contained sequence.
 *
 * @param data the encoded data. Array of longs, each storing up to 16 nts, with optional padding at the end.
 *             The data is left-aligned inside the long, starting from MSB.
 * @param size the size of this data represented (in NTs)
 */
final case class NTBitArray(data: Array[Long], size: Int) extends Comparable[NTBitArray] {
  import NTBitArray._

  override def toString: String = longsToString(data, 0, size)

  def toBinaryString: String = data.map(_.toBinaryString).mkString(" ")

  /** Represent this bit array as a right-aligned int.
   * Only well-defined if the value is small enough to fit in a positive integer (size <= 15).
   * toInt and [[NTBitArray.fromLong()]] are inverses as long as this holds true.
   */
  def toInt: Int =
    toLong.toInt

  /** Represent this bit array as a right-aligned long.
   * Only well-defined if the value is small enough to fit in a positive long. (size <= 31).
   * toLong and [[NTBitArray.fromLong()]] are inverses as long as this holds true.
   */
  def toLong: Long =
    data(0) >>> (64 - size * 2)

  def dataOrBlank(offset: Int): Long =
    if (offset < data.length) data(offset) else 0L

  /** Obtain the reverse complement of this NT bit array.  */
  def reverseComplement: NTBitArray = {
    /* reverse complement each long, then reverse the order of longs. */
    val l = data.length
    val r = new Array[Long](l)
    val shiftAmt = (size % 32) * 2

    var i = 0
    while (i < l) {
      r(l - i - 1) = BitRepresentation.reverseComplementLeftAligned(data(i), -1L)
      if (i == l - 1) {
        //zero out bits that aren't part of the data
        r(0) = r(0) & (-1L >>> (64 - shiftAmt))
      }
      i += 1
    }

    //The longs are now in the right order, but must be shifted around to ensure the result is
    //continuous and left-aligned
    i = 0
    if (shiftAmt != 0) { //if shiftAmt == 0, this algorithm isn't needed (and would zero out the data)
      while (i < l - 1) {
        r(i) = r(i) << (64 - shiftAmt) | (r(i + 1) >>> shiftAmt)
        i += 1
      }
      r(i) = r(i) << (64 - shiftAmt)
    }
    NTBitArray(r, size)
  }

  /** Return a new object that is either this array or its reverse complement, and that has
   * forward orientation. */
  def canonical: NTBitArray = {
    if (sliceIsForwardOrientation(0, size)) this.clone() else reverseComplement
  }

  def <(other: NTBitArray): Boolean = compareTo(other) < 0
  def >(other: NTBitArray): Boolean = compareTo(other) > 0

  /** Lexicographic comparison of two NT bit arrays. The arrays must have equal length. */
  def compareTo(other: NTBitArray): Int =
    compareTo(other, 0, data.length - 1)

  /** Ordering only well-defined when the arrays have equal size. This must be
   * guaranteed externally.  */
  @tailrec
  private def compareTo(other: NTBitArray, from: Int, max: Int): Int = {
    val cmp = java.lang.Long.compareUnsigned(data(from), other.data(from))
    if (cmp != 0) cmp
    else if (from < max) {
      compareTo(other, from + 1, max)
    }
    else 0
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: NTBitArray =>
        val cmp = compareTo(other)
        if (cmp != 0) false
        else size == other.size
      case _ => false
    }
  }

  override def hashCode(): Int =
    java.util.Arrays.hashCode(data) * 41 + size

  /** Bitwise and of NT bit arrays. Applies the operation up to the shortest length of the two.
   * Mutates this object to write the result.
   */
  def &= (other: NTBitArray): this.type = {
    var i = 0
    while (i < data.length && i < other.data.length) {
      data(i) = data(i) & other.data(i)
      i += 1
    }
    this
  }

  /** Bitwise xor of NT bit arrays. Applies the operation up to the shortest length of the two.
   * Mutates this object to write the result.
   */
  def ^= (other: NTBitArray): this.type = {
    var i = 0
    while (i < data.length && i < other.data.length) {
      data(i) = data(i) ^ other.data(i)
      i += 1
    }
    this
  }

  /** Bitwise or of NT bit arrays. Applies the operation up to the shortest length of the two.
   * Mutates this object to write the result.
   */
  def |= (other: NTBitArray): this.type = {
    var i = 0
    while (i < data.length && i < other.data.length) {
      data(i) = data(i) | other.data(i)
      i += 1
    }
    this
  }

  /** Bitwise left shift of NT bit arrays. Will not change the length of the array, so data will be lost.
   * Mutates this object to write the result.
   */
  def <<=(bits: Int): this.type = {
    var write = 0
    var shift = bits
    var read = 0
    //lose data initially
    while (shift > 63) {
      read += 1
      shift -= 64
    }

    while (write < data.length) {
      if (read < data.length - 1 && shift > 0) {
        //Note that >>> 64 has the same meaning as >>> 0, i.e. no change, so we must
        //treat it as a special case, hence the shift > 0 check
        data(write) = (data(read) << shift) | (data(read + 1) >>> (64 - shift))
      } else if (read < data.length) {
        data(write) = data(read) << shift
      } else {
        data(write) = 0
      }
      write += 1
      read += 1
    }
    this
  }

  /** Bitwise unsigned right shift of NT bit arrays. Will not change the length of the array, so data will be lost.
   * Mutates this object to write the result.
   */
  def >>>=(bits: Int): this.type = {
    var read = data.length - 1
    var shift = bits
    var write = data.length - 1

    //find the first long we can start reading from
    while (shift > 63 && write >= 0) {
      read -= 1
      shift -= 64
    }

    while (write >= 0) {
      if (read > 0 && shift > 0) {
        //Note that >>> 64 has the same meaning as >>> 0, i.e. no change, so we must
        //treat it as a special case, hence the shift > 0 check
        data(write) = data(read) >>> shift | data(read - 1) << (64 - shift)
      } else if (read >= 0) {
        data(write) = data(read) >>> shift
      } else {
        data(write) = 0
      }
      read -= 1
      write -= 1
    }
    this
  }

  def clear(): Unit = {
    var i = 0
    while (i < data.length) {
      data(i) = 0
      i += 1
    }
  }

  override def clone(): NTBitArray = {
    val d = data.clone()
    NTBitArray(d, size)
  }

  /** Clone this NTBitArray with a new length, adding an AAAA suffix or truncating as needed. */
  def cloneWithLength(length: Int): NTBitArray = {
    val d = java.util.Arrays.copyOf(data, longsForSize(length))
    NTBitArray(d, length)
  }

  /** Shift this NT bit array one BP to the left, dropping one bp, and add one bp on the right,
   * maintaining the same length. Mutates in place. */
  def shiftAddBP(add: Byte): this.type = {
    shiftLongArrayKmerLeft(data, add, size)
    this
  }

  /**
   * Construct a new NTBitArray from a subsequence of this one, copying data from this object.
   */
  def sliceAsCopy(sliceStart: Int, sliceLength: Int): NTBitArray = {
    val data = sliceAsLongArray(sliceStart, sliceLength)
    NTBitArray(data, sliceLength)
  }

  /**
   * Test the orientation of a slice of this buffer.
   * @param pos Start position
   * @param sliceSize Length of slice (must be an odd number)
   * @return True iff this slice has forward orientation.
   */
  def sliceIsForwardOrientation(pos: Int, sliceSize: Int): Boolean = {
    var st = pos
    var end = pos + sliceSize - 1
    while (st < end) {
      val a = apply(st)
      val b = complementOne(apply(end))
      if (a < b) return true
      if (a > b) return false

      st += 1
      end -= 1
    }
    //Here, st == end
    //Resolve a nearly palindromic case, such as: AACTT whose r.c. is AAGTT
    apply(st) < G
  }

  /**
   * Obtain the (twobit) NT at a given position.
   * Only the lowest two bits of the byte are valid. The others will be zeroed out.
   */
  def apply(pos: Int): Byte = {
    val lng = pos / 32
    val lval = data(lng)
    val localOffset = pos % 32
    ((lval >>> (2 * (31 - localOffset))) & 0x3).toByte
  }

  /** Obtain all k-mers from this bit array as long arrays.
   *
   * @param k length of k-mers
   * @param orientation orientation filter for k-mers
   * @return All k-mers as an iterator
   */
  def kmersAsLongArrays(k: Int, orientation: Orientation = Both): Iterator[Array[Long]] =
    KmerTable.fromSegment(this, BuildParams(k, orientation, sort = false)).iterator

  /**
   * Write all k-mers from this bit array into a KmerTableBuilder.
   * @param destination builder to write to
   * @param k k
   * @param orientation orientation filter for k-mers
   * @param provider function to generate extra (tag) data for the k-mer starting at each column (offset). By
   *                        default no extra data is generated.
   */
  def writeKmersToBuilder(destination: KmerTableBuilder, k: Int, orientation: Orientation,
                          provider: RowTagProvider = EmptyRowTagProvider): Unit = {
    val lastKmer = sliceAsLongArray(0, k)
    var i = 0
    if (provider.isPresent(i) && (orientation == Both || sliceIsForwardOrientation(i, k))) {
      destination.addLongs(lastKmer)
      provider.writeForCol(i, destination)
    }
    i += 1
    while (i < NTBitArray.this.size - k + 1) {
      if (provider.isPresent(i) && (orientation == Both || sliceIsForwardOrientation(i, k))) {
        shiftLongKmerAndWrite(lastKmer, apply(i - 1 + k), k, destination)
        provider.writeForCol(i, destination)
      } else {
        shiftLongArrayKmerLeft(lastKmer, apply(i - 1 + k), k)
      }
      i += 1
    }
  }

  /** Create a long array representing a subsequence of this sequence.
   *
   * @param offset 0-based offset
   * @param sliceSize   size
   */
  def sliceAsLongArray(offset: Int, sliceSize: Int): Array[Long] = {
    val buf = longBuffer(sliceSize)
    copySliceAsLongArray(buf, offset, sliceSize)
    buf
  }

  /** Write a subsequence of this sequence to the provided long array.
   *
   * @param writeTo destination to write to (at offset zero)
   * @param offset offset to read from (0-based)
   * @param partSize amount to write
   */
  def copySliceAsLongArray(writeTo: Array[Long], offset: Int, partSize: Int): Unit = {
    val shiftAmt = (offset % 32) * 2

    val finalKeepBits = if (partSize % 32 == 0) 64 else (partSize % 32) * 2
    val finalLongMask = -1L << (64 - finalKeepBits)

    val numLongs = if (partSize % 32 == 0) { partSize / 32 } else { partSize / 32 + 1 }
    val sourceLongs = data.length
    var i = 0
    var read = offset / 32
    while (i < numLongs) {
     var x = data(read) << shiftAmt
      if (read < sourceLongs - 1 && shiftAmt > 0) {
        x = x | (data(read + 1) >>> (64 - shiftAmt))
      }
      if (i == numLongs - 1) {
        x = x & finalLongMask
      }
      writeTo(i) = x
      read += 1
      i += 1
    }
  }
}
