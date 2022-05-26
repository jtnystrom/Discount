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

package com.jnpersson.discount.bucket

import com.jnpersson.discount.Abundance

/**
 * Statistics for a single bin/bucket.
 * @param id Minimizer/hash (human-readable)
 * @param superKmers Total number of superkmers
 * @param totalAbundance Total number of k-mers counted
 * @param distinctKmers Total number of distinct k-mers
 * @param uniqueKmers Total number of k-mers with abundance == 1
 * @param maxAbundance Greatest abundance seen for a single k-mer
 */
final case class BucketStats(id: String, superKmers: Long, totalAbundance: Abundance, distinctKmers: Long,
                             uniqueKmers: Long, maxAbundance: Abundance) {
  def merge(other: BucketStats): BucketStats = {
    BucketStats(id, superKmers + other.superKmers,
      totalAbundance + other.totalAbundance,
      distinctKmers + other.distinctKmers,
      uniqueKmers + other.uniqueKmers,
      if (maxAbundance > other.maxAbundance) maxAbundance else other.maxAbundance
    )
  }
}

object BucketStats {

  /**
   * Collect all statistics except super-kmers
   * @param id Human-readable ID of the bucket
   * @param counts Counts for each k-mer in the bucket
   * @return Aggregate statistics for the bucket
   */
  def collectFromCounts(id: String, counts: Iterator[Abundance]): BucketStats = {
    var totalAbundance: Abundance = 0
    var distinctKmers: Abundance = 0
    var uniqueKmers: Abundance = 0
    var maxAbundance: Abundance = 0

    for (item <- counts) {
      totalAbundance += item
      distinctKmers += 1
      if (item == 1) { uniqueKmers += 1 }
      if (item > maxAbundance) { maxAbundance = item }
    }
    BucketStats(id, 0, totalAbundance, distinctKmers, uniqueKmers, maxAbundance)
  }
}