/*
 * This file is part of Discount. Copyright (c) 2022 Johan Nyström-Persson.
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


package com.jnpersson.discount.hash

import com.jnpersson.discount.{NTSeq, SeqID, SeqLocation, SeqTitle}
import com.jnpersson.discount.util.ZeroNTBitArray

import scala.collection.BitSet

/**
 * A sequence fragment with a controlled maximum size. Does not contain whitespace.
 * @param header
 * @param location 1-based location in the source sequence
 * @param nucleotides
 */
final case class InputFragment(header: SeqTitle, location: SeqLocation, nucleotides: NTSeq)

/**
 * A hashed segment (i.e. a superkmer, where every k-mer shares the same minimizer)
 * with minimizer, sequence ID, and 1-based sequence location
 *
 * @param hash        hash (minimizer)
 * @param sequence    Sequence ID/header
 * @param location    Sequence location (1-based) if available
 * @param nucleotides Encoded nucleotides of this segment
 */
final case class SplitSegment(hash: BucketId, sequence: SeqID, location: SeqLocation, nucleotides: ZeroNTBitArray) {

  /**
   * Obtain a human-readable (decoded) version of this SplitSegment
   * @param splitter The splitter object that generated this segment
   * @return
   */
  def humanReadable(splitter: MinSplitter): (String, SeqID, SeqLocation, NTSeq) =
    (splitter.humanReadable(hash), sequence, location, nucleotides.toString)

}

object MinSplitter {
  /** Estimated bin size (sampled count of a minimizer, scaled up) that is considered a "large" bucket
   * This can be used to help determine the best counting method. */
  val largeThreshold = 5000000

  val INVALID = -1
}

/**
 * Split reads into superkmers by ranked motifs (minimizers). Such superkmers can be bucketed by the corresponding
 * minimizer.
 * @param space
 * @param k
 */
final case class MinSplitter(space: MotifSpace, k: Int) {
  if (space.largeBuckets.length > 0) {
    println(s"${space.largeBuckets.length} motifs are expected to generate large buckets.")
  }

  @transient
  lazy val scanner = space.scanner

  /** Split a read into superkmers.
   * @param read the read to split
   * @param addRC whether to add the reverse complement read on the fly
   * @return an iterator of (position in read, rank (hash/minimizer ID), encoded superkmer,
   *         location in sequence if available)
   */
  def splitEncode(read: NTSeq, addRC: Boolean = false): Iterator[(Int, Int, ZeroNTBitArray, SeqLocation)] = {
    val enc = scanner.allMatches(read)
    val part1 = splitRead(enc._1, enc._2)
    if (addRC) {
      part1 ++ splitRead(enc._1, true)
    } else {
      part1
    }
  }

  /** Split an encoded read into superkmers.
   * @param encoded the read to split
   * @return an iterator of (position in read, rank (hash/minimizer ID), encoded superkmer,
   *         location in sequence if available)
   */
  def splitRead(encoded: ZeroNTBitArray, reverseComplement: Boolean = false):
    Iterator[(Int, Int, ZeroNTBitArray, SeqLocation)] = {
    val enc = scanner.allMatches(encoded, reverseComplement)
    splitRead(enc._1, enc._2)
  }

  /**
   * Split a read into superkmers, and return them together with the corresponding minimizer.
   * @param encoded the read to split
   * @param matches discovered motif ranks in the superkmer
   * @return an iterator of (position in read, rank (hash/minimizer ID), encoded superkmer,
   *         location in sequence if available)
   */
  def splitRead(encoded: ZeroNTBitArray, matches: Array[Int]): Iterator[(Int, Int, ZeroNTBitArray, SeqLocation)] = {
    val window = new PosRankWindow(space.width, k, matches)

    var regionStart = 0
    new Iterator[(Int, Int, ZeroNTBitArray, SeqLocation)] {
      def hasNext: Boolean = window.hasNext

      def next: (Int, Int, ZeroNTBitArray, SeqLocation) = {
        val p = window.next
        val rank = matches(p)

        if (rank == MinSplitter.INVALID) {
          throw new Exception(
            s"""|Found a window with no motif in a read. Is the supplied motif set valid?
                |Erroneous read without motif in a window: $encoded
                |Matches found: ${matches.toList}
                |""".stripMargin)
        }

        var consumed = 1
        while (window.hasNext && window.head == p) {
          window.next
          consumed += 1
        }

        val thisStart = regionStart
        regionStart += consumed

        if (window.hasNext) {
          val segment = encoded.sliceAsCopy(thisStart, consumed + (k - 1))
          (p - space.width, rank, segment, thisStart)
        } else {
          val segment = encoded.sliceAsCopy(thisStart, encoded.size - thisStart)
          (p - space.width, rank, segment, thisStart)
        }
      }
    }
  }

  /** Split a read into super-mers, efficiently encoding them in binary form in the process,
    also preserving sequence ID and location.  */
  def splitEncodeLocation(read: InputFragment, sequenceIDs: Map[SeqTitle, SeqID]): Iterator[SplitSegment] =
    for {
      (pos, rank, ntseq, location) <- splitEncode(read.nucleotides)
    } yield SplitSegment(rank, sequenceIDs(read.header), read.location + location, ntseq)

  /** Compute a human-readable form of the bucket ID. */
  def humanReadable(id: BucketId): NTSeq =
    space.byPriority(id.toInt)
}
