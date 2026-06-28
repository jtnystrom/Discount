/*
 * This file is part of Discount. Copyright (c) 2019-2023 Johan Nyström-Persson.
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

package com.jnpersson.discount.spark

import com.jnpersson.discount._
import com.jnpersson.discount.bucket.{BucketStats, Reducer, ReducibleBucket, Tag}
import com.jnpersson.discount.hash.BucketId
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{collect_list, explode, udf}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.util.SplittableRandom

object Index {
  import org.apache.spark.sql._

  def randomTableName: String = {
    val rnd = scala.util.Random.nextLong()
    val useRnd = if (rnd < 0) - rnd else rnd
    s"discount_$useRnd"
  }

  def read(location: String, knownParams: Option[IndexParams] = None)(implicit spark: SparkSession): Index =
    synchronized {
      //This method is synchronized to avoid clashing on the name 'buckets'

    import spark.sqlContext.implicits._

    val useLocation = HDFSUtil.makeQualified(location)
    val params = knownParams.getOrElse(IndexParams.read(useLocation))

    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS buckets")
    spark.sql(s"""|CREATE TABLE buckets(id long, supermers array<struct<data: array<long>, size: int>>,
                  |  tags array<array<int>>)
                  |USING PARQUET CLUSTERED BY (id) INTO ${params.buckets} BUCKETS
                  |LOCATION '$useLocation'
                  |""".stripMargin)
    val bkts = spark.sql("SELECT * FROM buckets").as[ReducibleBucket]
    new Index(params, bkts)
  }

  def write(data: DataFrame, location: String, numBuckets: Int): Unit = {
    println(s"Saving index into $numBuckets partitions")


    //A unique table name is needed to make saveAsTable happy, but we will not need it again
    //when we read the index back (by HDFS path)
    val tableName = randomTableName
    /*
     * Use saveAsTable instead of ordinary parquet save to preserve buckets/partitioning.
     */
    data.
      write.mode(SaveMode.Overwrite).
      option("path", location).
      bucketBy(numBuckets, "id").
      saveAsTable(tableName)
  }

  val random = new SplittableRandom()

  /** Construct a new counting index from the given sequences. K-mers will not be normalized.
   * @param compatible A compatible index to copy parameters from
   * @param reads Sequences to index
   */
  def fromNTSeqs(compatible: Index, reads: Dataset[NTSeq])(implicit spark: SparkSession): Index =
    GroupedSegments.
      fromReads(reads, Simple, false, compatible.params.bcSplit).
      toIndex(Both, compatible.params.buckets)

  /** Construct a new counting index from the given sequences. K-mers will not be normalized.
   * This method is not intended for large amounts of data, as everything has to go through the Spark driver.
   * @param compatible A compatible index to copy parameters from
   * @param reads Sequences to index
   */
  def fromNTSeqs(compatible: Index, reads: Seq[NTSeq])(implicit spark: SparkSession): Index = {
    import spark.sqlContext.implicits._
    fromNTSeqs(compatible, reads.toDS())
  }

  /** Construct a new counting index from the given sequence. K-mers will not be normalized.
   * This method is not intended for large amounts of data, as everything has to go through the Spark driver.
   * @param compatible A compatible index to copy parameters from
   * @param read Sequence to index
   */
  def fromNTSeq(compatible: Index, read: NTSeq)(implicit spark: SparkSession): Index =
    fromNTSeqs(compatible, List(read))


  /** Split buckets into supermer/tag pairs according to a new minimizer ordering, constructing new
   * buckets from the result. The resulting buckets will have shorter supermers but will respect the new ordering.
   * @param input buckets to split
   * @param reducer reducer to use for compacting the result
   * @param spl a splitter that reflects the new ordering
   * */
  def reSplitBuckets(input: Dataset[ReducibleBucket], reducer: Reducer, spl: Broadcast[AnyMinSplitter])
                    (implicit spark: SparkSession): Dataset[ReducibleBucket] = {
    import spark.sqlContext.implicits._
    implicit val enc = Encoders.tuple(Encoders.product[ReducibleBucket], Helpers.encoder(spl.value))

    val segments = for {
      bucket <- input
      splitter = spl.value
      (sm, tags) <- bucket.supermers zip bucket.tags
      (_, rank, segment, pos) <- splitter.splitRead(sm)
      segmentTags = tags.slice(pos.toInt, pos.toInt + segment.size - (splitter.k - 1))
    } yield (HashSegment(rank.toLong, segment), segmentTags)

    val buckets = segments.groupBy($"_1.hash".as("id")).
      agg(collect_list("_1.segment").as("supermers"),
        collect_list("_2").as("tags")).as[ReducibleBucket]

    //sort supermers by length, favouring long supermers over short ones when compacting
    buckets.map(b => {
      val supermersTags = (b.supermers zip b.tags).sortWith(_._2.length > _._2.length).unzip
      b.copy(supermers = supermersTags._1, tags = supermersTags._2).
        reduceCompact(reducer)
    })
  }
}

/**
 * A bucketed k-mer index. Indexes store super-mers in a Dataset of [[ReducibleBucket]],
 *  where each k-mer is associated with a tag. Typically tags are k-mer counts, and then the Index becomes a multiset
 *  of counted k-mers.
 *  Indexes are immutable, like other Spark datastructures, and operations like filtering return a new Index rather
 *  than change the existing one in place.
 *  Indexes can be combined using operations like union, intersect, and subtract, and can be written to disk in various
 *  formats. The default format used by the write() and read() methods is bucketed parquet files, which gives
 *  good data compression and avoids shuffling when the same Index is used repeatedly.
 *
 * @param params Index parameters, which define the minimizer scheme, the lengths of k and m,
 * and the number of buckets. Two indexes must have compatible parameters to be combined.
 * @param buckets K-mer buckets. Buckets contain super-mers and tags for each k-mer. Tags can be,
 *                for example, k-mer counts.
 */
class Index(val params: IndexParams, val buckets: Dataset[ReducibleBucket])
              (implicit spark: SparkSession)  {
  import Index._
  import spark.sqlContext.implicits._

  def bcSplit = params.bcSplit

  /** Cache this index by caching the underlying dataset. This will persist it in memory and on disk (if needed),
   * which means that it does not have to be recomputed again if used repeatedly. See [[Dataset.cache]]. */
  def cache(): this.type = { buckets.cache(); this }

  /** Unpersist this index, undoing the effect of caching. See [[Dataset.unpersist]]. */
  def unpersist(): Unit = { buckets.unpersist() }

  /** Obtain counts for these k-mers.
   * @param orientation orientation filter for k-mers
   */
  def counted(orientation: Orientation = Both): CountedKmers =
    new CountedKmers(buckets, orientation, bcSplit)

  /** Obtain per-bucket (bin) statistics. */
  def stats(min: Option[Int] = None, max: Option[Int] = None): Dataset[BucketStats] = {
    val bcSplit = this.bcSplit
    filterCounts(min, max).buckets.select("id", "tags").as[(BucketId, Array[Array[Tag]])].map {
      case (hash, abundances) =>
        BucketStats.collectFromCounts(bcSplit.value.humanReadable(hash), abundances)
    }
  }

  def totalStats(min: Option[Int] = None, max: Option[Int] = None): BucketStats =
    stats(min, max).collect().foldLeft(BucketStats.empty)(_ merge _)

  /**
   * Obtain these counts as a histogram.
   * @return Pairs of abundances and their frequencies in the dataset.
   */
  def histogram: Dataset[(Tag, Long)] = {
    val exp1 = buckets.select(explode($"tags").as("exp1"))
    exp1.select(explode($"exp1").as("abundance")).
      where($"abundance" =!= 0).
      groupBy("abundance").count().sort("abundance").as[(Tag, Long)]
  }

  /**
   * Write the histogram of this data to HDFS.
   * This action triggers a computation.
   * @param output Directory to write to (prefix name)
   */
  def writeHistogram(output: String): Unit =
    Output.writeTSV(histogram, output)

  /** Write per-bucket statistics to HDFS.
   * This action triggers a computation.
   * @param location Directory (prefix name) to write data to
   */
  def writeBucketStats(location: String): Unit = {
    val bkts = stats()
    bkts.cache()
    bkts.filter(_.superKmers > 0).
      write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${location}_bucketStats")
    Output.showStats(bkts, Some(location))
    bkts.unpersist()
  }

  /** Show summary stats for this index.
   * This action triggers a computation.
   */
  def showStats(outputLocation: Option[String] = None): Unit = {
    Output.showStats(stats(), outputLocation)
  }

  /** Write this index to a location.
   * This action triggers a computation.
   */
  def write(location: String): Unit = {
    Index.write(buckets.toDF(), location, params.buckets)
    params.write(location, s"Properties for Index $location")
  }

  /** Union this index with another one, combining the k-mers using the given reducer type.
   * A k-mer is kept after a union operation if it is present in either of the input indexes, and passes
   * any other rules that the reducer implements. */
  def union(other: Index, rule: Rule): Index = {
    params.compatibilityCheck(other.params, strict = true)
    val k = bcSplit.value.k

    //Alias the id columns to prevent a cartesian product (as Spark will do for trivially true join conditions)
    //if we do a self join
    val b1 = buckets.select($"id", $"supermers", $"tags", $"id".as("id1"))
    val b2 = other.buckets.select($"id", $"supermers", $"tags", $"id".as("id2"))

    //The join type here is the default inner join, not an outer join as we might expect for a union operation.
    //However, we guarantee that each minimizer (id) occurs exactly once in each index, which allows this to work
    //correctly. Using inner join is important as it can avoid shuffles on bucketed tables.
    val joint = b1.joinWith(b2, b1("id1") === b2("id2"))

    val makeBucket =
      udf((b1: Option[ReducibleBucket], b2: Option[ReducibleBucket]) =>
        ReducibleBucket.unionCompact(b1, b2, k, rule))

    //Preserve the id column to avoid shuffling later on
    val joint2 = joint.toDF("b1", "b2").
      select($"b1.id".as("id"), makeBucket($"b1", $"b2").as("bucket")).
      select($"id", $"bucket.supermers".as("supermers"), $"bucket.tags".as("tags")).
      as[ReducibleBucket]
    new Index(params, joint2)
  }

  /** Intersect this index with another one, combining the k-mers using the given reducer type.
   * A k-mer is kept after an intersection operation if it is present in both of the input indexes, and passes
   * any other rules that the reducer implements. */
  def intersect(other: Index, rule: Rule): Index = {
    params.compatibilityCheck(other.params, strict = true)
    val k = bcSplit.value.k

    //Alias the id columns to prevent a cartesian product (as Spark will do for trivially true join conditions)
    //if we do a self join
    val b1 = buckets.select($"id", $"supermers", $"tags", $"id".as("id1"))
    val b2 = other.buckets.select($"id", $"supermers", $"tags", $"id".as("id2"))

    val makeBucket =
      udf((b1: ReducibleBucket, b2: ReducibleBucket) =>
        ReducibleBucket.intersectCompact(b1, b2, k, rule))

    //Preserve the id column to avoid shuffling later on
    val joint = b1.joinWith(b2, b1("id1") === b2("id2"))
    val joint2 = joint.toDF("b1", "b2").
      select($"b1.id".as("id"), makeBucket($"b1", $"b2").as("bucket")).
      select($"id", $"bucket.supermers".as("supermers"), $"bucket.tags".as("tags")).
      as[ReducibleBucket]
    new Index(params, joint2)
  }

  /** Look up the given NT sequences (strings) in this index, if they exist. Convenience method.
   * This is equivalent to intersect(Index.fromNTSeqs(sequences, params), Rule.Left).
   * This method is not intended for large amounts of data, as everything has to go through the Spark driver. */
  def lookup(sequences: Seq[String]): Index =
   lookup(Index.fromNTSeqs(this, sequences))

  /** Subtract another index from this one, using e.g. [[Rule.KmersSubtract]] or
   * [[Rule.CountersSubtract]]. Subtraction is implemented as a union, but this is not a commutative operation
   * due to how the rules are implemented.
   */
  def subtract(other: Index, rule: Rule): Index =
    union(other, rule)

  /** Union this index with a series of indexes using the given reducer type. */
  def unionMany(ixs: Iterable[Index], rule: Rule): Index =
    ixs.fold(this)(_.union(_, rule))

  /** Intersect this index with a series of indexes using the given reducer type. */
  def intersectMany(ixs: Iterable[Index], rule: Rule): Index =
    ixs.fold(this)(_.intersect(_, rule))

  /**
   * Subtract a series of indexes B1, B2... Bn from this index (A):
   * ((A - B1) - B2) - ...
   * using [[Rule.KmersSubtract]] or [[Rule.CountersSubtract]]. */
  def subtractMany(ixs: Iterable[Index], rule: Rule): Index =
    ixs.foldLeft(this)(_.subtract(_, rule))

  /** Transform the tags of this index, returning a copy with the changes applied */
  def mapTags(f: Tag => Tag): Index = {
    //This function does not filter supermers since that would be too heavyweight (compaction can be done separately)

    //Mutate tags in place, no need to allocate new objects
    def mapF(tags: Array[Array[Tag]]): Array[Array[Tag]] = {
      var i = 0
      while (i < tags.length) {
        var j = 0
        val row = tags(i)
        while (j < row.length) {
          row(j) = f(row(j))
          j += 1
        }
        i += 1
      }
      tags
    }

    val mapper = udf(mapF(_))

    val newBuckets = buckets.select($"id", $"supermers", mapper($"tags").as("tags")).
      as[ReducibleBucket]
    new Index(params, newBuckets)
  }

  //Pure Spark SQL function for negating tags, returning a new index.
  //NB this is currently not as fast as mapTags above, but an interesting example of how to use the transform function
  private def negateTags(): Index = {
    val newBuckets = buckets.selectExpr("id", "supermers",
      "transform(tags, xs -> transform(xs, x -> -x )) as tags").
    as[ReducibleBucket]
    new Index(params, newBuckets)
  }

  def filterCounts(min: Int, max: Int): Index = {
    val reducer = Reducer.union(bcSplit.value.k)
    if (min == abundanceMin && max == abundanceMax) {
      this
    } else {
      mapTags(t => {
        if (t >= min && t <= max) t else reducer.zeroValue
      })
    }
  }

  /** Filter counts in this index based on lower and/or upper bound */
  def filterCounts(min: Option[Int] = None, max: Option[Int] = None): Index =
    filterCounts(min.getOrElse(abundanceMin), max.getOrElse(abundanceMax))

  /** Sample k-mers from this index.
   * Sampling is done on the level of distinct k-mers. K-mers will either be included with the same count as before,
   * or omitted. */
  def sample(fraction: Double): Index = {
    val reducer = Reducer.union(bcSplit.value.k)
    mapTags(t => if (random.nextDouble() < fraction) { t } else { reducer.zeroValue } )
  }

  /** Split the super-mers according to a new minimizer ordering,
   * generating an index with the same k-mers that respects the new ordering. */
  def changeMinimizerOrdering(spl: Broadcast[AnyMinSplitter]): Index = {
    val reducer = Reducer.union(spl.value.k)
    new Index(params.copy(bcSplit = spl), Index.reSplitBuckets(buckets, reducer, spl))
  }

  /** Construct a compatible index (suitable for operations like intersection and union) from the given
   * sequence files. Settings will be copied from this index.
   * The count method in the Discount object (pregrouped/simple) will be used, defaulting to Simple if none was specified.
   * The minimizer scheme (splitter) used for this index will be reused.
   *
   * @param discount Source of settings such as count method and input format. k must be the same as in this index.
   * @param inFiles Input files (fasta/fastq etc)
   */
  def newCompatible(discount: Discount, inFiles: String*): Index = {
    val useMethod = discount.method.resolve(bcSplit.value.priorities)
    val inputs = discount.getInputSequences(inFiles, useMethod.addRCToMainData(discount))
    GroupedSegments.fromReads(inputs, useMethod, discount.normalize, bcSplit).
      toIndex(discount.orientationFilter, params.buckets)
  }

  /** Repartition this index into a different number of partitions (and buckets when written to disk as parquet) */
  def repartition(partitions: Int): Index =
    new Index(params.copy(buckets = partitions), buckets.repartition(partitions, $"id"))

  //Below are some convenience methods for API use.

  /** Convenience method to filter counts by minimum */
  def filterMin(min: Int): Index =
    filterCounts(Some(min), None)

  /** Convenience method to filter counts by maximum */
  def filterMax(max: Int): Index =
    filterCounts(None, Some(max))

  /** Convenience method for intersection using [[Rule.Min]] */
  def intersectMin(other: Index): Index =
    intersect(other, Rule.Min)

  /** Convenience method for intersection using [[Rule.Max]] */
  def intersectMax(other: Index): Index =
    intersect(other, Rule.Max)

  /** Look up the given k-mers in this index, if they exist. Convenience method. This is equivalent to
   * intersect(query, Rule.Left). */
  def lookup(query: Index): Index =
    intersect(query, Rule.Left)

  /** Convenience method for intersection using [[Rule.Left]] */
  def intersectLeft(other: Index): Index =
    intersect(other, Rule.Left)

  /** Convenience method for intersection using [[Rule.Right]] */
  def intersectRight(other: Index): Index =
    intersect(other, Rule.Right)

  /** Convenience method for union using [[Rule.Max]] */
  def unionMax(other: Index): Index =
    union(other, Rule.Max)

  /** Convenience method for union using [[Rule.Min]] */
  def unionMin(other: Index): Index =
    union(other, Rule.Min)

  /** Convenience method for union using [[Rule.Left]] */
  def unionLeft(other: Index): Index =
    union(other, Rule.Left)

  /** Convenience method for union using [[Rule.Right]] */
  def unionRight(other: Index): Index =
    union(other, Rule.Right)

  /** Convenience method for union using [[Rule.Sum]] */
  def add(other: Index): Index =
    union(other, Rule.Sum)

  /** Convenience method for subtraction using [[Rule.CountersSubtract]] */
  def subtractCounts(other: Index): Index =
    subtract(other, Rule.CountersSubtract)

  /** Convenience method for subtraction using [[Rule.KmersSubtract]] */
  def subtractKmers(other: Index): Index =
    subtract(other, Rule.KmersSubtract)
}
