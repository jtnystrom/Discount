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


package com.jnpersson.discount.hash

import com.jnpersson.discount.{NTSeq, SeqID, SeqLocation, SeqTitle}
import com.jnpersson.discount.util.{ZeroNTBitArray}

final case class InputFragment(header: SeqTitle, location: SeqLocation, nucleotides: NTSeq)

/**
 * Split reads into superkmers by ranked motifs (minimizers). Such superkmers can be bucketed by the corresponding
 * minimizer.
 * @param space
 * @param k
 */
final case class MinSplitter(space: MotifSpace, k: Int) {
  @transient
  lazy val scanner = space.scanner

  /**
   * Split a read into superkmers, and return them together with the corresponding minimizer.
   */
  def splitEncode(read: NTSeq): Iterator[(Motif, ZeroNTBitArray)] = {
    val (encoded, matches) = scanner.allMatches(read)
    val window = new PosRankWindow(space.width, k, matches)

    var regionStart = 0
    new Iterator[(Motif, ZeroNTBitArray)] {
      def hasNext: Boolean = window.hasNext

      def next: (Motif, ZeroNTBitArray) = {
        val p = window.next
        val rank = matches(p)

        if (rank == Motif.INVALID) {
          throw new Exception(
            s"""|Found a window with no motif in a read. Is the supplied motif set valid?
                |Erroneous read without motif in a window: $read
                |Matches found: ${scanner.allMatches(read)._2.toList}
                |""".stripMargin)
        }

        var consumed = 1
        while (window.hasNext && window.head == p) {
          window.next
          consumed += 1
        }
        val features = scanner.featuresByPriority(rank)
        val thisStart = regionStart
        regionStart += consumed

        if (window.hasNext) {
          val segment = encoded.sliceAsCopy(thisStart, consumed + (k - 1))
          (Motif(p - space.width, features), segment)
        } else {
          val segment = encoded.sliceAsCopy(thisStart, read.length - thisStart)
          (Motif(p - space.width, features), segment)
        }
      }
    }
  }

  /**
   * Split the read into superkmers overlapping by (k-1) bases.
   * @param read
   * @return Pairs of (hash, superkmer)
   */
  @deprecated("It is preferred to use splitEncode instead.", "Sep 2021")
  def split(read: NTSeq): Iterator[(Motif, NTSeq)] = {
    splitEncode(read).map(x => (x._1, x._2.toString))
  }

  /**
   * Convert a hashcode into a compact representation.
   * @param hash
   * @return
   */
  def compact(hash: Motif): BucketId =
    hash.features.rank

  /** Compute a human-readable form of the Motif. */
  def humanReadable(hash: Motif): NTSeq =
    hash.pattern

  /** Compute a human-readable form of the bucket ID. */
  def humanReadable(id: BucketId): NTSeq =
    space.byPriority(id.toInt)
}
