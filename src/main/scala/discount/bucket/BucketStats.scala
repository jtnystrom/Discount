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

package discount.bucket

import discount.Abundance

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
   * @param counts Counts for each k-mer in a bucket
   * @return Aggregate statistics for the bucket
   */
  def collectFromCounts(id: String, counts: Iterator[Abundance]): BucketStats = {
    var totalAbundance = 0L
    var distinctKmers = 0L
    var uniqueKmers = 0L
    var maxAbundance = 0L

    for (item <- counts) {
      totalAbundance += item
      distinctKmers += 1
      if (item == 1) { uniqueKmers += 1 }
      if (item > maxAbundance) { maxAbundance = item }
    }
    BucketStats(id, 0, totalAbundance, distinctKmers, uniqueKmers, maxAbundance)
  }
}