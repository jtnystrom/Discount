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

package com.jnpersson.discount.bucket

import com.jnpersson.discount.{Both, Orientation}
import com.jnpersson.discount.util.KmerTable
import com.jnpersson.discount.spark.Rule

/** K-mer reduction parameters
 * @param k The length of k-mers
 * @param orientation Orientation of k-mers to keep in the result
 * @param intersect Whether the reduction is an intersection type (if not, it's a union)
 */
final case class ReduceParams(k: Int, orientation: Orientation = Both, intersect: Boolean = false)

/**
 * A method for combining identical k-mers (which may have associated extra data)
 */
trait Reducer {
  def params: ReduceParams

  def k: Int = params.k

  /** Orientation filter for k-mers */
  def orientation: Orientation = params.orientation

  val tagOffset: Int

  /** Preprocess bucket A prior to op(A,B) */
  def preprocessFirst(bucket: ReducibleBucket): ReducibleBucket = bucket

  /** Preprocess bucket B prior to op(A,B) */
  def preprocessSecond(bucket: ReducibleBucket): ReducibleBucket = bucket

  /**
   * Apply a binary operation op(into, from) on the tags of the k-mers at these positions,
   * writing the result in the tags of "into", writing the zero value into the tags of "from".
   * This method will only be called on equal k-mers.
   *
   * @param table The table containing k-mers
   * @param into Target k-mer index in the table (k-mer A in op(A,B))
   * @param from Source k-mer index in the table (k-mer B in op(A,B))
   */
  def reduceEqualKmers(table: KmerTable, into: Int, from: Int): Unit

  /**
   * A special value that is used to indicate non-existent/redundant k-mers.
   * Should also have the property that op(A, zero) == op(zero, A) for all A
   */
  val zeroValue: Tag = 0

  /**
   * Whether to keep the k-mer (together with tag data) after reduction.
   * @param table The table containing k-mers
   * @param kmer Index of k-mer to be tested
   * @return
   */
  def shouldKeep(table: KmerTable, kmer: Int): Boolean = true

  def hasNonZeroTag(tags: Array[Tag]): Boolean = {
    var i = 0
    while (i < tags.length) {
      if (tags(i) != zeroValue) return true
      i += 1
    }
    false
  }

  /**
   * Reduce tags of a single KmerTable by combining equal k-mers. Requires that the table was sorted
   * at construction time. Mutates the table.
   * @param table the KmerTable
   */
  def reduceKmers(table: KmerTable): KmerTable = {
    val it = table.indexIterator.buffered
    while (it.hasNext) {
      val thisKmer = it.next()
      while (it.hasNext && table.compareKmers(thisKmer, table, it.head) == 0) {
        reduceEqualKmers(table, thisKmer, it.next())
      }
    }
    table
  }
}

/** A reducer that handles k-mer count values stored in the longsForK(k) + 1 tag position. */
trait CountReducer extends Reducer {

  val tagOffset: Int = KmerTable.longsForK(k) + 1

  def reduceEqualKmers(table: KmerTable, into: Int, from: Int): Unit = {
    //Remove any keep flag that may have been set previously
    //Note: some reducers need to be able to pass negative values through here
    val count1 = table.kmers(tagOffset)(into).toInt
    val count2 = table.kmers(tagOffset)(from).toInt

    //Note: this check may no longer be needed, as we don't add zero count k-mers into the KmerTables any longer
    if (count1 != 0 && count2 != 0) {
      val keep = 1L

      //Toggle the keep flag to indicate that a successful comparison between two nonzero count
      //equal k-mers occurred (criterion to keep the k-mer after intersection)
      table.kmers(tagOffset)(into) = (keep << 32) | reduceCounts(count1, count2)
      //Discard this k-mer on compaction
      table.kmers(tagOffset)(from) = 0
    }
  }

  def reduceCounts(count1: Tag, count2: Tag): Tag

  override def shouldKeep(table: KmerTable, kmer: Int): Boolean = {
    if (params.intersect) {
      table.kmers(tagOffset)(kmer) >> 32 != 0
    } else {
      table.kmers(tagOffset)(kmer) != 0
    }
  }
}


object Reducer {
  import Rule._

  /** Convert a Long to Int without overflowing Int.MaxValue */
  def cappedLongToInt(x: Long): Int =
    if (x > Int.MaxValue) Int.MaxValue else x.toInt

  def parseRule(rule: String): Rule = rule match {
    case "sum" => Sum
    case "max" => Max
    case "min" => Min
    case "left" => Left
    case "right" => Right
    case "counters_subtract" => CountersSubtract
    case "kmers_subtract" => KmersSubtract
  }

  /** Configure a union Reducer.
   * @param k The length of k-mers
   * @param orientation k-mer orientation filter
   * @param reduction The reduction rule
   */
  def union(k: Int, reduction: Rule = Sum, orientation: Orientation = Both): Reducer =
    configure(ReduceParams(k, orientation, intersect = false), reduction)

  /** Configure a Reducer.
   * @param params    Reduction parameters
   * @param reduction The reduction rule
   */
  def configure(params: ReduceParams, reduction: Rule): Reducer = {
    reduction match {
      case Sum => SumReducer(params)
      case Max => MaxReducer(params)
      case Min => MinReducer(params)
      case Left => LeftReducer(params)
      case Right => RightReducer(params)
      case CountersSubtract => CountersSubtractReducer(params)
      case KmersSubtract =>
        assert(!params.intersect)
        KmerSubtractReducer(params)
    }
  }
}

/** Implements the [[com.jnpersson.discount.spark.Rule.Sum]] reduction rule */
final case class SumReducer(params: ReduceParams) extends CountReducer {

  //Overflow check, since we are generating a new value
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    Reducer.cappedLongToInt(count1.toLong + count2.toLong)
}

/** Implements the [[com.jnpersson.discount.spark.Rule.CountersSubtract]] reduction rule.
 * For each k-mer we calculate count_1 - count_2 and set the result to this value.
 * Only positive counts are preserved in the output. */
final case class CountersSubtractReducer(params: ReduceParams) extends CountReducer {

  //Negate tags (counts) on the right hand side
  //Note that both values are expected to be positive initially.
  override def preprocessSecond(bucket: ReducibleBucket): ReducibleBucket =
    bucket.copy(tags = bucket.tags.map(xs => xs.map(- _)))

  //Overflow check, since we are generating a new value
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    Reducer.cappedLongToInt(count1.toLong + count2.toLong) //Effectively count1 + (-count2) which was already negated

  override def shouldKeep(table: KmerTable, kmer: Tag): Boolean = {
    if (params.intersect) {
      (table.kmers(tagOffset)(kmer) >> 32) != 0 &&
        table.kmers(tagOffset)(kmer) > 0
    } else {
      table.kmers(tagOffset)(kmer) > 0
    }
  }
}

/** Implements the [[com.jnpersson.discount.spark.Rule.KmersSubtract]] reduction rule.
 * k-mers are kept if they existed in bucket A, but not in bucket B. */
final case class KmerSubtractReducer(params: ReduceParams) extends CountReducer {
  //Intersection using this reducer is not meaningful, as it would always remove everything and produce an empty set.
  assert (!params.intersect)

  //Negate tags (counts) on the right hand side
  override def preprocessSecond(bucket: ReducibleBucket): ReducibleBucket =
    bucket.copy(tags = bucket.tags.map(xs => xs.map(- _)))

  //This method is only called if we saw the k-mer in both buckets, with a nonzero (but negative in the second)
  //count in each. In that case, the k-mer should not be kept.
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    0

  //Only k-mers that existed in the first bucket (only) will retain a positive tag at this point
  override def shouldKeep(table: KmerTable, kmer: Tag): Boolean =
    table.kmers(tagOffset)(kmer) > 0
}

/** Implements the [[com.jnpersson.discount.spark.Rule.Min]] reduction rule */
final case class MinReducer(params: ReduceParams) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    if (count1 < count2) count1 else count2
}

/** Implements the [[com.jnpersson.discount.spark.Rule.Max]] reduction rule */
final case class MaxReducer(params: ReduceParams) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    if (count1 > count2) count1 else count2
}

/** Implements the [[com.jnpersson.discount.spark.Rule.Left]] reduction rule */
final case class LeftReducer(params: ReduceParams) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    count1
}

/** Implements the [[com.jnpersson.discount.spark.Rule.Right]] reduction rule */
final case class RightReducer(params: ReduceParams) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    count2
}

