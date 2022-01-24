/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
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

import com.jnpersson.discount.NTSeq
import BitRepresentation._


import java.nio.ByteBuffer

object NTBitArray {
  import BitRepresentation._

  /**
   * Reversibly construct an array of 64-bit Longs that represent the data in
   * a nucleotide sequence. The 2*length leftmost bits in the array will be populated.
   * @param data
   * @return
   */
  def encode(data: NTSeq): ZeroNTBitArray = {
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
    ZeroNTBitArray(buf, data.length)
  }

  def longBuffer(size: Int): Array[Long] = {
    val numLongs = if (size % 32 == 0) { size >> 5 } else { (size >> 5) + 1 }
    new Array[Long](numLongs)
  }

  /**
   * Shift an array of two-bits one step to the left, dropping one bp, and inserting another on the right.
   * @return
   */
  def shiftLongArrayKmerLeft(data: Array[Long], newBP: Byte, k: Int): Unit = {
    val n = data.length
    var i = 0
    while (i < n - 1) {
      data(i) = (data(i) << 2) | (data(i + 1) >>> 62)
      i += 1
    }
    //i == n -1
    val kmod32 = k & 31
    data(i) = (data(i) << 2) | (newBP.toLong << ((32 - kmod32) * 2))
  }

  /**
   * Shift an array of two-bits one step to the left, dropping one bp, and inserting another on the right.
   * @return
   */
  def shiftLongKmerAndWrite(data: Array[Long], newBP: Byte, k: Int, destination: KmerTableBuilder): Unit = {
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
    val x = (data(i) << 2) | (newBP.toLong << ((32 - kmod32) * 2))
    data(i) = x
    destination.addLong(x)
  }

  /**
   * Decode a previously encoded NT sequence to human-readable string form.
   * @param data
   * @param offset offset in the data array to start from
   * @param size number of letters to decode
   * @return
   */
  def longsToString(data: Array[Long], offset: Int, size: Int): NTSeq = {
    val sb = new StringBuilder
    val bytes = if (size % 32 == 0) (size / 4) else (size / 4 + 8)
    val buf = ByteBuffer.allocate(bytes)
    longsToString(buf, sb, data, offset, size)
  }

  /**
   * Decode a previously encoded NT sequence to human-readable string form.
   * Optimized version for repeated calls (avoids repeatedly allocating new buffers)
   * @param data
   * @param offset offset in the data array to start from
   * @param size number of letters to decode
   * @return
   */
  def longsToString(buffer: ByteBuffer, builder: StringBuilder, data: Array[Long], offset: Int, size: Int): NTSeq = {
    buffer.clear()
    builder.clear()
    var i = 0
    while (i < data.length) {
      buffer.putLong(data(i))
      i += 1
    }
    BitRepresentation.bytesToString(buffer.array(), builder, offset, size)
  }
}

/**
 * A bit-packed array of nucleotides, where each letter is represented by two bits.
 */
trait NTBitArray {
  import NTBitArray._
  import BitRepresentation._

  /**
   * Array of longs, each storing up to 16 nts, with optional padding at the end.
   */
  def data: Array[Long]

  /**
   * Offset into the array where data starts (in NTs)
   */
  def offset: Int

  /**
   * Size of the data represented (in NTs)
   */
  def size: Int

  override def toString: String = longsToString(data, offset, size)

  /**
   * Construct a new NTBitArray from a subsequence of this one, sharing data with this object.
   */
  def slice(from: Int, length: Int): NTBitArray = OffsetNTBitArray(data, from, length)

  /**
   * Construct a new NTBitArray from a subsequence of this one, copying data from this object.
   */
  def sliceAsCopy(offset: Int, length: Int): ZeroNTBitArray = {
    val data = partAsLongArray(offset, length)
    ZeroNTBitArray(data, length)
  }

  /**
   * Test the orientation of a slice of this buffer.
   * @param pos Start position
   * @param size Length of slice (must be an odd number)
   * @return True iff this slice has forward orientation.
   */
  def sliceIsForwardOrientation(pos: Int, size: Int): Boolean = {
    var st = pos
    var end = pos + size - 1
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
    (apply(st) < G)
  }

  /**
   * Obtain the (twobit) NT at a given offset.
   * Only the lowest two bits of the byte are valid. The others will be zeroed out.
   */
  def apply(pos: Int): Byte = {
    val truePos: Int = offset + pos
    val lng = truePos / 32
    val lval = data(lng)
    val localOffset = truePos % 32
    ((lval >> (2 * (31 - localOffset))) & 0x3).toByte
  }

  /**
   * Obtain all k-mers from this bit array.
   * @param k
   * @param onlyForwardOrientation If this flag is true, only k-mers with forward orientation will be returned.
   * @return
   */
  def kmers(k: Int, onlyForwardOrientation: Boolean = false): Iterator[NTBitArray] =
    Iterator.range(offset, size - k + 1).
      filter(i => (!onlyForwardOrientation) || sliceIsForwardOrientation(i, k)).
      map(i => slice(i, k))

  def kmersAsLongArrays(k: Int, onlyForwardOrientation: Boolean = false): Iterator[Array[Long]] =
    KmerTable.fromSegment(this, k, onlyForwardOrientation, false).iterator

  /**
   * Obtain all k-mers from this bit array
   * @param k
   * @param onlyForwardOrientation If this flag is true, only k-mers with forward orientation will be returned.
   * @return
   */
  def writeKmersToBuilder(destination: KmerTableBuilder, k: Int, forwardOnly: Boolean,
                          extraDataForCol: Int => Array[Long] = x => Array.emptyLongArray) = {
    val lastKmer = partAsLongArray(offset, k)
    var i = offset
    if (!forwardOnly || sliceIsForwardOrientation(i, k)) {
      destination.addLongs(lastKmer)
      destination.addLongs(extraDataForCol(offset))
    }
    while (i < NTBitArray.this.size - k + 1) {
      if (i > offset) {
        if (!forwardOnly || sliceIsForwardOrientation(i, k)) {
          shiftLongKmerAndWrite(lastKmer, apply(i - 1 + k), k, destination)
          destination.addLongs(extraDataForCol(i))
        } else {
          shiftLongArrayKmerLeft(lastKmer, apply(i - 1 + k), k)
        }
      }
      i += 1
    }
  }

  /** Create a long array representing a subsequence of this sequence. */
  final def partAsLongArray(offset: Int, size: Int): Array[Long] = {
    val buf = longBuffer(size)
    copyPartAsLongArray(buf, offset, size)
    buf
  }

  /** Write a subsequence of this sequence to the provided long array. */
  def copyPartAsLongArray(writeTo: Array[Long], offset: Int, size: Int): Unit = {
    val shiftAmt = (offset % 32) * 2

    val finalKeepBits = if (size % 32 == 0) 64 else (size % 32) * 2
    val finalLongMask = -1L << (64 - finalKeepBits)

    val numLongs = if (size % 32 == 0) { size / 32 } else { size / 32 + 1 }
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

/** An NTBitArray that begins at some offset in its binary data */
final case class OffsetNTBitArray(data: Array[Long], offset: Int, size: Int) extends NTBitArray

/** An NTBitArray that begins at offset zero in its binary data */
final case class ZeroNTBitArray(data: Array[Long], size: Int) extends NTBitArray {
  def offset = 0
}