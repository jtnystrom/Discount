/**
 * Part of the Friedrich bioinformatics framework.
 * Copyright (C) Gabriel Keeble-Gagnere and Johan Nystrom-Persson.
 * Dual GPL/MIT license. Please see the files README and LICENSE for details.
 */
package discount.util

import java.nio.ByteBuffer

import scala.language.implicitConversions

/**
 * The BPBuffer stores base pairs as "twobits", which are pairs of bits.
 * The actual data is stored in the Byte array "data". Offset is the offset into this array
 * where the BPs begin, where 0-3 are offsets in the first byte, 4-7 in the second, and so on.
 * Size is the number of BPs stored by this BPBuffer, starting from the offset.
 * The maximum storage capacity is achieved by offset = 0 and size = data.size * 4.
 *
 * BPBuffers are immutable. Instead of modifying the underlying data, modifying operations
 * create new instances.
 * Whenever possible we try to avoid copying the underlying byte array, changing instead the offset
 * and size parameters.
 *
 * The internal storage layout of data is continuous left-to-right:
 * (... ... bp1 bp2) (bp3 bp4 bp5 bp6) (bp7 bp8 bp9 bp10) (... ...
 * for offset = 2, size = 10, where the parentheses delimit each individual byte.
 *
 * For operations such as sorting, ordering, and comparison, it is usually most efficient to
 * convert the BPBuffer to Array[Long] using the provided methods for this purpose.
 */
trait BPBuffer {

  /**
   * Number of BPs in this sequence.
   */
  def size: Int

  /**
   * Offset into the raw data array where this sequence starts (number of BPs).
   * Multiply by two to get bit offset.
   */
  def offset: Int

  /**
   * The raw data that backs this BPBuffer. This can contain more data than the sequence
   * itself, since it may be shared between multiple objects.
   * The sequence represented by a BPBuffer object will be a subsequence of the
   * one contained here.
   */
  def data: Array[Byte]

  /**
   * Construct a new bpbuffer from a subsequence of this one.
   */
  def slice(from: Int, length: Int): BPBuffer = BPBuffer.wrap(this, from, length)

  /**
   * Test the orientation of a slice of this buffer.
   * @param pos Start position
   * @param size Length of slice (must be an odd number)
   * @return True iff this slice has forward orientation.
   */
  def sliceIsForwardOrientation(pos: Int, size: Int): Boolean

  /**
   * Obtain all k-mers from this buffer.
   * @param k
   * @param onlyForwardOrientation If this flag is true, only k-mers with forward orientation will be returned.
   * @return
   */
  def kmers(k: Int, onlyForwardOrientation: Boolean = false): Iterator[BPBuffer] =
    (0 until (size - k + 1)).iterator.
      filter(i => (!onlyForwardOrientation) || sliceIsForwardOrientation(i, k)).
      map(i => slice(i, k))

  /**
   * Obtain all k-mers from this buffer, packed as Array[Long].
   * @param k
   * @param onlyForwardOrientation If this flag is true, only k-mers with forward orientation will be returned.
   * @return
   */
  def kmersAsLongArrays(k: Int, onlyForwardOrientation: Boolean = false): Iterator[Array[Long]] =
    (0 until (size - k + 1)).iterator.
      filter(i => (!onlyForwardOrientation) || sliceIsForwardOrientation(i, k)).
      map(i => partAsLongArray(i, k))


  /**
   * Create a long array representing a subsequence of this bpbuffer.
   * @param offset
   * @param size
   * @return
   */
  final def partAsLongArray(offset: Int, size: Int): Array[Long] = {
    val buf = longBuffer(size)
    copyPartAsLongArray(buf, offset, size)
    buf
  }

  def longBuffer(size: Int): Array[Long]

  def copyPartAsLongArray(writeTo: Array[Long], offset: Int, size: Int)

}

/**
 * A BPBuffer is a sequence of base pairs (nucleotides).
 */

object BPBuffer {

  import BitRepresentation._

  /**
   * Creates a new bpbuffer from an ACTG string.
   */
  def wrap(str: String): ZeroBPBuffer = {
    new ZeroBPBuffer(stringToBytes(str), str.length)
  }

  /**
   * Creates a new bpbuffer from an ACTG string, with a given 0-based starting offset
   * and size.
   */
  def wrap(str: String, offset: Int, size: Int): ForwardBPBuffer = {
    new ForwardBPBuffer(stringToBytes(str), offset, size)
  }

  /**
   * Creates a new bpbuffer from an existing bpbuffer.
   * This saves memory by reusing the objects from the first one.
   * For instance, if the old one is ACTGACTG, and we do
   * BPBuffer.wrap(x,2,4), then the result contains TGAC, but using the same backing
   * buffer.
   */

  def wrap(buffer: BPBuffer, offset: Int, size: Int): BPBuffer = {
    if (size % 4 == 0) {
      assert(buffer.data.length >= size / 4)
    } else {
      assert(buffer.data.length >= size / 4 - 1)
    }

    assert(size <= (buffer.size - offset - buffer.offset))
    assert(size > 0 && offset >= 0)

    buffer match {
      case fwd: BPBufferImpl   => new ForwardBPBuffer(fwd.data, fwd.offset + offset, size)
      case _                   => { throw new Exception("Unexpected buffer implementation") }
    }
  }

  def wrap(data: Array[Int], offset: Int, size: Int): BPBuffer = {
    val bb = ByteBuffer.allocate(data.length * 4)
    var i = 0
    while (i < data.length) {
      bb.putInt(data(i))
      i += 1
    }
    new ForwardBPBuffer(bb.array(), offset, size)
  }

  //Optimised version for repeated calls - avoids allocating a new buffer each time
  def longsToString(buffer: ByteBuffer, builder: StringBuilder, data: Array[Long], offset: Int, size: Int): String = {
    buffer.clear()
    builder.clear()
    var i = 0
    while (i < data.length) {
      buffer.putLong(data(i))
      i += 1
    }
    BitRepresentation.bytesToString(buffer.array(), builder, offset, size)
  }

  /**
   * One integer contains 4 bytes (16 bps). This function computes a single such integer from the
   * underlying backing buffer to achieve a simpler representation.
   * Main bottleneck in equality testing, hashing, ordering etc.
   */
  def computeIntArrayElement(data: Array[Byte], offset: Int, size: Int, i: Int): Int = {
    val os = offset
    val spo = size + os
    val shift = (os % 4) * 2
    val mask = (spo) % 4
    var res = 0

    val finalByte = (if (mask == 0) {
      (spo / 4) - 1
    } else {
      spo / 4
    })

    var pos = (os / 4) + i * 4
    var intshift = 32 + shift

    //adjust pos here, since some bytes may need to be used both for the final byte of one int
    //and for the first byte of the next
    pos -= 1

    var j = 0
    val dl = data.length
    while (j < 5 && pos + 1 <= finalByte) {
      //process 1 byte for each iteration

      intshift -= 8
      pos += 1

      var src = data(pos)

      //mask redundant bits from the final byte
      if (pos == finalByte) {
        if (mask == 1) {
          src = src & (0xfc << 4)
        } else if (mask == 2) {
          src = src & (0xfc << 2)
        } else if (mask == 3) {
          src = src & 0xfc
        }

      }
      var shifted = (src & 0xff)

      //adjust to position in the current int
      if (intshift > 0) {
        shifted = (shifted << intshift)
      } else {
        shifted = (shifted >> (-intshift))
      }

      //The bits should be mutually exclusive
      //				assert ((res & shifted) == 0)
      res |= shifted
      //				print(binint(newData(i)) + " ")

      j += 1
    }
    res
  }

  /**
   * The main implementation of the BPBuffer trait.
   */
  trait BPBufferImpl extends BPBuffer {

    /**
     * Returns a "twobit" in the form of a single byte.
     * Only the lowest two bits of the byte are valid. The others will be zeroed out.
     */
    final def directApply(pos: Int): Byte = {
      assert(pos >= 0 && pos < size)
      val truePos: Int = offset + pos
      val byte = truePos / 4
      val bval = data(byte)
      val localOffset = truePos % 4
      ((bval >> (2 * (3 - localOffset))) & 0x3)
    }

    def apply(pos: Int): Byte = {
      directApply(pos)
    }

    /**
     * Create an int array representing a subsequence of this bpbuffer.
     */
    def computeIntArray(offset: Int, size: Int): Array[Int] = {
      var i = 0
      val ns = ((size >> 4) + 1) //safely (?) going slightly over in some cases
      val newData = new Array[Int](ns)
      while (i < ns) {
        newData(i) = BPBuffer.computeIntArrayElement(data, offset, size, i)
        i += 1
      }
      newData
    }

    protected def computeIntArrayElement(pos: Int): Int = {
      BPBuffer.computeIntArrayElement(data, offset, size, pos)
    }

    def longBuffer(size: Int): Array[Long] = {
      val numLongs = ((size >> 5) + 1)
      new Array[Long](numLongs)
    }

    final def copyPartAsLongArray(writeTo: Array[Long], offset: Int, size: Int) {
      var write = 0
      var read = 0
      val numLongs = writeTo.size
      val numInts = ((size >> 4) + 1)

      while (write < numLongs && read < numInts) {
        var x = BPBuffer.computeIntArrayElement(data, offset, size, read).toLong << 32
        read += 1
        if (read < numInts) {
          //Because this ends up as the second part of the long, we have to preserve the sign bit in its place.
          //Integer.toUnsignedLong will do the trick.
          x = (x | Integer.toUnsignedLong(BPBuffer.computeIntArrayElement(data, offset, size, read)))
          read += 1
        }
        writeTo(write) = x
        write += 1
      }
    }

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
      //Resolve a palindromic case, such as: AACTT whose r.c. is AAGTT
      (apply(st) < G)
    }

    def toBPString: String = {
      bytesToString(data, new StringBuilder(size), offset, size)
    }

    override def toString(): String = toBPString
  }

  final case class ForwardBPBuffer(val data: Array[Byte], val offset: Int, val size: Int) extends BPBufferImpl

  /**
   * A forward BPBuffer that has zero offset. Useful to save space in serialized encodings.
   */
  final case class ZeroBPBuffer(val data: Array[Byte], val size: Int) extends BPBufferImpl {
    def offset = 0
  }
}
