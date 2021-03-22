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

import java.util.NoSuchElementException

import discount.NTSeq

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer


/**
 * Scans a single read, tracking the current motif set in a sliding window of length k.
 */
final class WindowExtractor(space: MotifSpace, scanner: ShiftScanner,
                            windowMotifs: TopRankCache, k: Int, read: NTSeq) {
  val matches = scanner.allMatches(read)
  var scannedToPos: Int = space.maxMotifLength - 2

  def motifAt(pos: Int): Motif = matches(pos)

  /**
   * Moves the k-length window we are considering in the read.
   * May only be called for monotonically increasing values of pos
   * pos is the final position of the window we scan to, inclusive.
   */
  private def scanTo(pos: Int) {
    while (pos > scannedToPos + 1) {
      //Catch up
      scanTo(scannedToPos + 1)
    }
    if (pos < scannedToPos) {
      throw new Exception("Invalid parameter, please supply increasing values of pos only")
    }

    if (pos > scannedToPos) {
      //pos == scannedToPos + 1
      scannedToPos = pos
      if (pos >= read.length()) {
        throw new Exception("Already reached end of read")
      }

      //Starting position of the potential motif we are looking for in the window that ends at pos(inclusive)
      val consider = pos - space.maxMotifLength + 1

      if (consider >= 0) {
        windowMotifs.moveWindowAndInsert(pos - k + 1, motifAt(consider))
      } else {
        windowMotifs.moveWindowAndInsert(pos - k + 1, Motif.Empty)
      }
    }
  }

  def scanAndGetTop(pos: Int): Motif = {
    scanTo(pos)
    windowMotifs.top
  }
}

/**
 * Split a read into superkmers by ranked motifs (minimizers).
 * @param space
 * @param k
 */
final case class MotifExtractor(space: MotifSpace, k: Int) extends ReadSplitter[Motif] {
  @transient
  lazy val scanner = new ShiftScanner(space)

  /**
   * Look for high priority motifs in regions of length >= k.
   * Returns the positions where each contiguous Motif region is first detected.
   */
  def regionsInRead(read: NTSeq): Iterator[(Motif, Int)] = {
    new Iterator[(Motif, Int)] {
      val ext = new WindowExtractor(space, scanner, new FastTopRankCache, k, read)
      var p = k - 1

      def hasNext = (p <= read.length - 1)

      def next: (Motif, Int) = {
        try {
          var scan = ext.scanAndGetTop(p)
          val lastMotif = scan
          val result = (lastMotif, p)
          p += 1
          //Consume all occurrences of lastMotif
          while ((scan eq lastMotif) && (p <= read.length - 1)) {
            scan = ext.scanAndGetTop(p)
            if (scan eq lastMotif) {
              p += 1
            }
          }
          result
        } catch {
          case nse: NoSuchElementException =>
            Console.err.println(s"After scan to position $p")
            Console.err.println("Erroneous read without motif: " + read)
            Console.err.println(s"Matches found: ${ext.matches}")
            throw new Exception("Found a window with no motif in a read. Is the supplied motif set valid?", nse)
        }
      }
    }
  }

  def split(read: NTSeq): Iterator[(Motif, NTSeq)] = {
    SplitterUtils.splitRead(k, read, regionsInRead(read))
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
   * which overlap by (k-1) bases. Their ordering is not guaranteed.
   */
  def splitRead[T](k: Int, read: NTSeq, buckets: Iterator[(T, Int)]): Iterator[(T, NTSeq)] = {
    val buf = buckets.buffered

    new Iterator[(T, NTSeq)] {
      def hasNext = buf.hasNext
      def next: (T, NTSeq) = {
        val b1 = buf.next
        if (buf.hasNext) {
          val b2 = buf.head
          (b1._1, read.substring(b1._2 - (k - 1), b2._2))
        } else {
          (b1._1, read.substring(b1._2 - (k - 1)))
        }
      }
    }
  }
}