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

package discount.bucket


/**
 * Statistics about all the k-mers contained in one bucket.
 */
final case class BucketStats(sequences: Long, totalAbundance: Long, distinctKmers: Long,
                             uniqueKmers: Long, maxAbundance: Long) {
  def merge(other: BucketStats): BucketStats = {
    BucketStats(sequences + other.sequences,
      totalAbundance + other.totalAbundance,
      distinctKmers + other.distinctKmers,
      uniqueKmers + other.uniqueKmers,
      if (maxAbundance > other.maxAbundance) maxAbundance else other.maxAbundance
    )
  }
}

object BucketStats {

  /**
   * Collect all statistics except sequences
   * @param counts Counts for each k-mer in a bucket
   * @return Aggregate statistics for the bucket
   */
  def collectFromCounts(counts: Iterator[Long]): BucketStats = {
    val all = counts.
      foldLeft(BucketStats(0, 0, 0, 0, 0))((acc, item) => {
        BucketStats(0,
        acc.totalAbundance + item,
        acc.distinctKmers + 1,
        if (item == 1) acc.uniqueKmers + 1 else acc.uniqueKmers,
        if (item > acc.maxAbundance) item else acc.maxAbundance
      )
    })
    all
  }
}