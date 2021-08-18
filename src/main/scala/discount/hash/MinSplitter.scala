/*
 * This file is part of Discount. Copyright (c) 2020 Johan Nyström-Persson.
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

package discount.hash

import discount.NTSeq
import discount.util.{NTBitArray, ZeroNTBitArray}

/**
 * Split reads into superkmers by ranked motifs (minimizers).
 * @param space
 * @param k
 */
final case class MinSplitter(space: MotifSpace, k: Int) extends ReadSplitter[Motif] {
  @transient
  lazy val scanner = space.scanner

  /**
   * Obtain the top ranked motif for each k-length window in a read.
   */
  def slidingTopMotifs(read: NTSeq): Iterator[Motif] = {
    val matches = scanner.allMatches(read)
    val windowMotifs = new PosRankWindow

    if (read.length < k) {
      Iterator.empty
    } else {
      var pos = space.width - k
      matches.map(m => {
        windowMotifs.moveWindowAndInsert(pos, m)
        pos += 1
        windowMotifs.top
      }).
        drop(k - space.width) //The first items do not correspond to a full k-length window
    }
  }

  /**
   * Look for regions of length >= k.
   * Returns the positions where each contiguous Motif region is first detected.
   */
  def regionsInRead(read: NTSeq): Iterator[(Motif, Int)] = {
    new Iterator[(Motif, Int)] {
      val topMotifs = slidingTopMotifs(read).buffered
      var regionStart = -1

      def hasNext = topMotifs.hasNext

      def next: (Motif, Int) = {
        val lastMotif = topMotifs.next
        if (lastMotif eq Motif.Empty) {
          throw new Exception(
            s"""|Found a window with no motif in a read. Is the supplied motif set valid?
                |Erroneous read without motif in a window: $read
                |Matches found: ${scanner.allMatches(read).toList}
                |""".stripMargin)
        }

        regionStart += 1
        var consumed = 0
        //Consume all consecutive occurrences of lastMotif, as they belong to the same region
        while (topMotifs.hasNext && (topMotifs.head eq lastMotif)) {
          topMotifs.next()
          consumed += 1
        }
        regionStart += consumed
        (lastMotif, regionStart - consumed)
      }
    }
  }

  def split(read: NTSeq): Iterator[(Motif, NTSeq)] = {
    SplitterUtils.splitRead(k, read, regionsInRead(read))
  }

  def splitEncode(read: NTSeq): Iterator[(Motif, ZeroNTBitArray)] = {
    SplitterUtils.splitEncodeRead(k, read, regionsInRead(read))
  }

  /**
   * Convert a hashcode into a compact representation.
   * @param hash
   * @return
   */
  def compact(hash: Motif): BucketId =
    hash.features.rank

  override def humanReadable(hash: Motif): String =
    hash.pattern

  override def humanReadable(id: BucketId): String =
    space.byPriority(id.toInt)
}

object SplitterUtils {

  /**
   * Convert extracted buckets (motifs and their positions) into substrings of a read,
   * which overlap by (k-1) bases.
   */
  def splitRead[T](k: Int, read: NTSeq, buckets: Iterator[(T, Int)]): Iterator[(T, NTSeq)] = {
    val buf = buckets.buffered

    new Iterator[(T, NTSeq)] {
      def hasNext = buf.hasNext
      def next: (T, NTSeq) = {
        val b1 = buf.next
        if (buf.hasNext) {
          val b2 = buf.head
          (b1._1, read.substring(b1._2, b2._2 + (k - 1)))
        } else {
          (b1._1, read.substring(b1._2))
        }
      }
    }
  }

  /**
   * Convert extracted buckets (motifs and their positions) into substrings of a read,
   * which overlap by (k-1) bases. Encode the substrings in binary form.
   */
  def splitEncodeRead[T](k: Int, read: NTSeq, buckets: Iterator[(T, Int)]): Iterator[(T, ZeroNTBitArray)] = {
    val buf = buckets.buffered
    val enc = NTBitArray.encode(read)

    new Iterator[(T, ZeroNTBitArray)] {
      def hasNext = buf.hasNext
      def next: (T, ZeroNTBitArray) = {
        val b1 = buf.next
        if (buf.hasNext) {
          val b2 = buf.head

          (b1._1, enc.sliceAsCopy(b1._2,  (b2._2 - b1._2) + k - 1))
        } else {
          (b1._1, enc.sliceAsCopy(b1._2, enc.size - b1._2))
        }
      }
    }
  }
}