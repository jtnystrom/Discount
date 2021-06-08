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
   * Collect all statistics except sequences
   * @param counts Counts for each k-mer in a bucket
   * @return Aggregate statistics for the bucket
   */
  def collectFromCounts(id: String, counts: Iterator[Abundance]): BucketStats = {
    val all = counts.
      foldLeft(BucketStats(id, 0, 0, 0, 0, 0))((acc, item) => {
        BucketStats(id, 0,
        acc.totalAbundance + item,
        acc.distinctKmers + 1,
        if (item == 1) acc.uniqueKmers + 1 else acc.uniqueKmers,
        if (item > acc.maxAbundance) item else acc.maxAbundance
      )
    })
    all
  }
}