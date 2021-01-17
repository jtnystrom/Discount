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
 * Scans a single read, using mutable state to track the current motif set
 * in a window.
 */
final class WindowExtractor(space: MotifSpace, scanner: ShiftScanner,
                            windowMotifs: TopRankCache, k: Int, read: NTSeq) {
  val matches = scanner.allMatches(read)
  var scannedToPos: Int = space.maxMotifLength - 2

  def motifAt(pos: Int): Motif = matches(pos)

  /**
   * May only be called for monotonically increasing values of pos
   * pos is the final position of the window we scan to, inclusive.
   */
  def scanTo(pos: Int): List[Motif] = {
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
        val motif = motifAt(consider)
        if (!(motif eq Motif.Empty)) {
          windowMotifs :+= motif
        }
      }

      windowMotifs.dropUntilPosition(pos - k + 1)
    }
    windowMotifs.takeByRank
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
   * Look for high priority motifs in a read.
   * Returns the positions where each contiguous Motif region is first detected
   */
  def regionsInRead(read: NTSeq): ArrayBuffer[(Motif, Int)] = {
    if (read.length < k) {
      return ArrayBuffer.empty
    }

    val perBucket = new ArrayBuffer[(Motif, Int)](read.length)

    val ext = new WindowExtractor(space, scanner, new FastTopRankCache, k, read)
    var p = k - 1

    var lastMotif: Motif = Motif.Empty

    try {
      while (p <= read.length - 1) {
        val scan = ext.scanTo(p).head
        if (!(scan eq lastMotif)) {
          lastMotif = scan
          perBucket += ((lastMotif, p))
        }
        p += 1
      }
      perBucket
    } catch {
      case nse: NoSuchElementException =>
        Console.err.println(s"After scan to position $p")
        Console.err.println("Erroneous read without motif: " + read)
        Console.err.println(s"Matches found: ${ext.matches}")
        throw new Exception("Found a window with no motif in a read. Is the supplied motif set valid?", nse)
    }
  }

  def split(read: NTSeq): Iterator[(Motif, NTSeq)] = {
    val bkts = regionsInRead(read).toList
    SplitterUtils.splitRead(k, read, bkts).iterator
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
   * Convert extracted buckets into overlapping substrings of a read,
   * overlapping by (k-1) bases. The ordering is not guaranteed.
   * Designed to operate on the list produced by the regionsInRead function.
   */

  def splitRead[T](k: Int, read: NTSeq, buckets: List[(T, Int)]): List[(T, NTSeq)] =
    splitRead(k, read, buckets, Nil)

  @tailrec
  def splitRead[T](k: Int, read: NTSeq, buckets: List[(T, Int)],
                acc: List[(T, NTSeq)]): List[(T, NTSeq)] = {
    buckets match {
      case b1 :: b2 :: bs =>
        splitRead(k, read, b2 :: bs, (b1._1, read.substring(b1._2 - (k - 1), b2._2)) :: acc)
      case b1 :: bs => (b1._1, read.substring(b1._2 - (k - 1))) :: acc
      case _ => acc
    }
  }
}