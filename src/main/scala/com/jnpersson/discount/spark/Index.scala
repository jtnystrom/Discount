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

package com.jnpersson.discount.spark

import com.jnpersson.discount._
import com.jnpersson.discount.bucket.{BucketStats, Reducer, ReducibleBucket, Tag}
import com.jnpersson.discount.hash.{BucketId, MinSplitter, MinTable}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{array, collect_list, explode, lit, udf}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.util.SplittableRandom

object Index {
  import org.apache.spark.sql._

  def read(location: String, knownParams: Option[IndexParams] = None)(implicit spark: SparkSession): Index = {
    import spark.sqlContext.implicits._
    val params = knownParams.getOrElse(IndexParams.read(location))

    //Does not delete the table itself, only removes it from the hive catalog
    //This is to ensure that we get the one in the expected location
    spark.sql("DROP TABLE IF EXISTS buckets")
    spark.sql(s"""|CREATE TABLE buckets(id long, supermers array<struct<data: array<long>, size: int>>,
                  |  tags array<array<int>>)
                  |USING PARQUET CLUSTERED BY (id) INTO ${params.buckets} BUCKETS
                  |LOCATION '$location'
                  |""".stripMargin)
    val bkts = spark.sql("SELECT * FROM buckets").as[ReducibleBucket]
    new Index(params, bkts)
  }

  def write(data: DataFrame, location: String, numBuckets: Int): Unit = {
    println(s"Saving index into $numBuckets partitions")

    val rnd = scala.util.Random.nextLong()
    val useRnd = if (rnd < 0) - rnd else rnd

    //A unique table name is needed to make saveAsTable happy, but we will not need it again
    //when we read the index back (by HDFS path)
    val tableName = s"hypercut_$useRnd"

    /*
     * Use saveAsTable instead of ordinary parquet save to preserve buckets/partitioning.
     */
    data.
      write.mode(SaveMode.Overwrite).
      option("path", location).
      bucketBy(numBuckets, "id").
      saveAsTable(tableName)
  }

  def getIndexSplitter(location: String, k: Int)(implicit spark: SparkSession): AnyMinSplitter = {
    val minLoc = s"${location}_minimizers.txt"
    val use = (new Sampling).readMotifList(minLoc)
    println(s"${use.length} motifs will be used (loaded from $minLoc)")
    MinSplitter(MinTable.using(use.toIndexedSeq), k)
  }

  /**
   * An iterator over all the k-mers in one bucket paired with abundances.
   * The Index must have already been compacted with a counting reducer.
   */
  private def countIterator(b: ReducibleBucket, normalize: Boolean, k: Int) =
  //Since 0-valued k-mers are not present in the index, but represent gaps in supermers,
  //we have to filter them out here.
    for { (sm, tags) <- b.supermers.iterator zip b.tags.iterator
          (km, count) <- sm.kmersAsLongArrays(k, normalize) zip tags.iterator
          if count > 0 }
      yield (km, count.toLong)

  val random = new SplittableRandom()

  /** Construct a new counting index from the given sequences. K-mers will not be normalized.
   * @param reads Sequences to index
   * @param params Index parameters. The location field will be ignored, so it is safe to reuse a parameter
   *               object from an existing index.
   */
  def fromNTSeqs(reads: Dataset[NTSeq], params: IndexParams)(implicit spark: SparkSession): Index = {
    val needleSegments = GroupedSegments.fromReads(reads, Simple(false), params.bcSplit)
    needleSegments.toIndex(false, params.buckets)
  }

  /** Construct a new counting index from the given sequences. K-mers will not be normalized.
   * @param reads Sequences to index
   * @param params Index parameters. The location field will be ignored, so it is safe to reuse a parameter
   *               object from an existing index.
   */
  def fromNTSeqs(reads: Seq[NTSeq], params: IndexParams)(implicit spark: SparkSession): Index = {
    import spark.sqlContext.implicits._
    fromNTSeqs(reads.toDS(), params)
  }

  /** Construct a new counting index from the given sequence. K-mers will not be normalized.
   * @param read Sequence to index
   * @param params Index parameters. The location field will be ignored, so it is safe to reuse a parameter
   *               object from an existing index.
   */
  def fromNTSeq(read: NTSeq, params: IndexParams)(implicit spark: SparkSession): Index =
    fromNTSeqs(List(read), params)


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
    } yield (HashSegment(rank, segment), segmentTags)

    val buckets = segments.groupBy($"_1.hash".as("id")).
      agg(collect_list("_1.segment").as("supermers"),
        collect_list("_2").as("tags")).as[ReducibleBucket]

    //sort supermers by length, favouring long supermers over short ones when compacting
    buckets.map(b => {
      val supermersTags = (b.supermers zip b.tags).sortWith(_._2.length > _._2.length).unzip
      b.copy(supermers = supermersTags._1, tags = supermersTags._2).
        compact(reducer)
    })
  }
}

/**
 * A bucketed k-mer index.
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

  def cache(): this.type = { buckets.cache(); this }
  def unpersist(): Unit = { buckets.unpersist() }

  def checkpoint(): Index = new Index(params, buckets.checkpoint())

  /** Obtain counts for these k-mers.
   *
   * @param normalize Whether to filter k-mers by orientation
   */
  def counted(normalize: Boolean = false):
    CountedKmers = {
    val k = bcSplit.value.k

    val counts = buckets.flatMap(countIterator(_, normalize, k))
    new CountedKmers(counts, bcSplit)
  }

  /** Obtain per-bucket (bin) statistics. */
  def stats(min: Option[Abundance] = None, max: Option[Abundance] = None): Dataset[BucketStats] = {
    val bcSplit = this.bcSplit
    filterCounts(min, max).buckets.map { case ReducibleBucket(hash, segments, abundances) =>
      BucketStats.collectFromCounts(bcSplit.value.humanReadable(hash), abundances)
    }
  }

  /**
   * Obtain these counts as a histogram.
   * @return Pairs of abundances and their frequencies in the dataset.
   */
  def histogram: Dataset[(Abundance, Long)] = {
    val exp1 = buckets.select(explode($"tags").as("exp1"))
    exp1.select(explode($"exp1").as("abundance")).
      where($"abundance" =!= 0).
      groupBy("abundance").count().sort("abundance").as[(Abundance, Long)]
  }

  /**
   * Write the histogram of this data to HDFS.
   * @param output Directory to write to (prefix name)
   */
  def writeHistogram(output: String): Unit =
    Counting.writeTSV(histogram, output)

  /** Write per-bucket statistics to HDFS.
   *
   * @param location Directory (prefix name) to write data to
   */
  def writeBucketStats(location: String): Unit = {
    val bkts = stats()
    bkts.cache()
    bkts.write.mode(SaveMode.Overwrite).option("sep", "\t").csv(s"${location}_bucketStats")
    Counting.showStats(bkts, Some(location))
    bkts.unpersist()
  }

  /** Show summary stats for this index */
  def showStats(outputLocation: Option[String] = None): Unit = {
    Counting.showStats(stats(), outputLocation)
  }

  def write(location: String)(implicit spark: SparkSession): Unit = {
    Index.write(buckets.toDF(), location, params.buckets)
    params.write(location, s"Properties for Index $location")
  }

  def union(other: Index, reducer: Reducer.Type): Index = {
    val k = bcSplit.value.k

    //The join type here is the default inner join, not an outer join as we might expect for a union operation.
    //However, we guarantee that each minimizer (id) occurs exactly once in each index, which allows this to work
    //correctly. Using inner join is important as it can avoid shuffles on bucketed tables.
    val (joint, red) = reducer match {
      case Reducer.Diff =>
        val negated = other.mapTags(x => -x)
        val joint = buckets.joinWith(negated.buckets, buckets("id") === negated.buckets("id"))
        (joint, Reducer.Sum)
      case _ =>
        val joint = buckets.joinWith(other.buckets, buckets("id") === other.buckets("id"))
        (joint, reducer)
    }

    val makeBucket =
      udf((b1: Option[ReducibleBucket], b2: Option[ReducibleBucket]) =>
        ReducibleBucket.mergeCompact(b1, b2, k, red))

    //Preserve the id column to avoid shuffling later on
    val joint2 = joint.toDF("b1", "b2").
      select($"b1.id".as("id"), makeBucket($"b1", $"b2").as("bucket")).
      select($"id", $"bucket.supermers".as("supermers"), $"bucket.tags".as("tags")).
      as[ReducibleBucket]
    new Index(params, joint2)
  }

  def intersect(other: Index, reducer: Reducer.Type): Index = {
    val k = bcSplit.value.k

    val makeBucket =
      udf((b1: ReducibleBucket, b2: ReducibleBucket) =>
        ReducibleBucket.intersectCompact(b1, b2, k, reducer))

    //Preserve the id column to avoid shuffling later on
    val joint = buckets.joinWith(other.buckets, buckets("id") === other.buckets("id"))
    val joint2 = joint.toDF("b1", "b2").
      select($"b1.id".as("id"), makeBucket($"b1", $"b2").as("bucket")).
      select($"id", $"bucket.supermers".as("supermers"), $"bucket.tags".as("tags")).
      as[ReducibleBucket]
    new Index(params, joint2)
  }

  def unionAll(ixs: Iterable[Index], reducer: Reducer.Type): Index =
    (this :: ixs.toList).reduce(_.union(_, reducer))

  def intersectAll(ixs: Iterable[Index], reducer: Reducer.Type): Index =
    (this :: ixs.toList).reduce(_.intersect(_, reducer))

  /** Transform the tags of this index, returning a new one.
   * Incurs the cost of using a UDF.  */
  def mapTags(f: Tag => Tag): Index = {
    //This function does not filter supermers since that would be too heavyweight (compaction can be done separately)

    //Mutate tags in place
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

  //Optimized function for negating tags, returning a new index.
  //NB this is currently not as fast as mapTags above.
  def negateTags(): Index = {
    val newBuckets = buckets.selectExpr("id", "supermers",
      "transform(tags, xs -> transform(xs, x -> -x )) as tags").
    as[ReducibleBucket]
    new Index(params, newBuckets)
  }

  def filterCounts(min: Abundance, max: Abundance): Index = {
    val reducer = Reducer.forK(bcSplit.value.k, false)
    if (min == abundanceMin && max == abundanceMax) {
      this
    } else {
      mapTags(t => {
        if (t >= min && t <= max) t else reducer.zeroValue
      })
    }
  }

  def filterCounts(min: Option[Abundance] = None, max: Option[Abundance] = None): Index =
    filterCounts(min.getOrElse(abundanceMin), max.getOrElse(abundanceMax))

  /** Sample k-mers from (potentially) all buckets in this index.
   * Sampling is done on the level of distinct k-mers. K-mers will either be included with the same count as before,
   * or omitted. */
  def sample(fraction: Double): Index = {
    val reducer = Reducer.forK(bcSplit.value.k, false)
    //TODO change the way sampling is being done - alter the tag instead
    mapTags(t => if (random.nextDouble() < fraction) { t } else { reducer.zeroValue } )
  }

  /** Split the super-mers according to a new minimizer ordering,
   * generating an index with the same k-mers that respects the new ordering. */
  def changeMinimizerOrdering(spl: Broadcast[AnyMinSplitter]): Index = {
    val reducer = Reducer.forK(spl.value.k, forwardOnly = false)
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
    val useMethod = discount.method.getOrElse(Simple(discount.normalize))
    val inputs = discount.getInputSequences(inFiles, useMethod.addRCToMainData)
    GroupedSegments.fromReads(inputs, useMethod, bcSplit).
      toIndex(discount.normalize, params.buckets)
  }

}
