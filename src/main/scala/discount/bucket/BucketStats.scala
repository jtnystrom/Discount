package discount.bucket

/**
 * Statistics about all the k-mers contained in one bucket.
 */
final case class BucketStats(sequences: Long, totalAbundance: Long, kmers: Long,
                            uniqueKmers: Long, maxAbundance: Long)

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
        acc.kmers + 1,
        if (item == 1) acc.uniqueKmers + 1 else acc.uniqueKmers,
        if (item > acc.maxAbundance) item else acc.maxAbundance
      )
    })
    all
  }
}