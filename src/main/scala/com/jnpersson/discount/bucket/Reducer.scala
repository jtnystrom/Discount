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

import com.jnpersson.discount.util.KmerTable

/**
 * A method for combining identical k-mers (which may have associated extra data)
 */
trait Reducer {
  def k: Int

  /**
   * Whether to include only canonical (forward oriented) k-mers when reducing
   */
  def forwardOnly: Boolean

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
}

/** A reducer that handles k-mer count values stored in the longsForK(k) + 1 tag position. */
trait CountReducer extends Reducer {
  def intersect: Boolean

  val tagOffset: Int = KmerTable.longsForK(k) + 1

  def reduceEqualKmers(table: KmerTable, into: Int, from: Int): Unit = {
    //Remove any keep flag that may have been set previously
    //Note: some reducers need to be able to pass negative values through here
    val count1 = table.kmers(tagOffset)(into).toInt
    val count2 = table.kmers(tagOffset)(from).toInt

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
    if (intersect) {
      table.kmers(tagOffset)(kmer) >> 32 != 0
    } else {
      table.kmers(tagOffset)(kmer) != 0
    }
  }
}


object Reducer {

  /** Convert a Long to Int without overflowing Int.MaxValue */
  def cappedLongToInt(x: Long): Int =
    if (x > Int.MaxValue) Int.MaxValue else x.toInt

  /**
   * k-mer combination (reduction) rules for combining indexes.
   * Most of these support both intersection and union. An intersection is an operation that requires
   * the k-mer to be present in every input index, or it will not be present in the output. A union may preserve
   * the k-mer even if it is present in only one input index.
   * Except for the case of the union Sum reduction, indexes must be compacted prior to reduction, that is, each
   * k-mer must occur in each index with a nonzero value only once.
   *
   * These rules were inspired by the design of KMC3: https://github.com/refresh-bio/KMC
   */
  sealed trait Rule extends Serializable

  /** Add k-mer counts together */
  object Sum extends Rule
  /** Select the maximum value */
  object Max extends Rule
  /** Select the minimum value */
  object Min extends Rule
  /** Select the first value */
  object Left extends Rule
  /** Select the second value */
  object Right extends Rule
  /** Subtract k-mer counts A-B, preserving positive results. */
  object CountersSubtract extends Rule
  /** Preserve only those k-mers that were present in A but absent in B (weaker version of subtract)
   * This does not support intersection, since the result would always be empty. */
  object KmersSubtract extends Rule

  def parseRule(rule: String): Rule = rule match {
    case "sum" => Sum
    case "max" => Max
    case "min" => Min
    case "left" => Left
    case "right" => Right
    case "counters_subtract" => CountersSubtract
    case "kmers_subtract" => KmersSubtract
  }

  def union(k: Int, forwardOnly: Boolean, reduction: Rule = Sum): Reducer =
    configure(k, forwardOnly, intersect = false, reduction)

  def configure(k: Int, forwardOnly: Boolean, intersect: Boolean, reduction: Rule): Reducer = {
    reduction match {
      case Sum => SumReducer(k, forwardOnly, intersect)
      case Max => MaxReducer(k, forwardOnly, intersect)
      case Min => MinReducer(k, forwardOnly, intersect)
      case Left => LeftReducer(k, forwardOnly, intersect)
      case Right => RightReducer(k, forwardOnly, intersect)
      case CountersSubtract => CountersSubtractReducer(k, forwardOnly, intersect)
      case KmersSubtract =>
        assert(!intersect)
        KmerSubtractReducer(k, forwardOnly)
    }
  }
}

final case class SumReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {

  //Overflow check, since we are generating a new value
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    Reducer.cappedLongToInt(count1.toLong + count2.toLong)
}

final case class CountersSubtractReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {

  //Negate tags (counts) on the right hand side
  //Note that both values are expected to be positive initially.
  override def preprocessSecond(bucket: ReducibleBucket): ReducibleBucket =
    bucket.copy(tags = bucket.tags.map(xs => xs.map(- _)))

  //Overflow check, since we are generating a new value
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    Reducer.cappedLongToInt(count1.toLong + count2.toLong) //count2 has already been negated

  override def shouldKeep(table: KmerTable, kmer: Tag): Boolean = {
    if (intersect) {
      (table.kmers(tagOffset)(kmer) >> 32) != 0 &&
        table.kmers(tagOffset)(kmer) > 0
    } else {
      table.kmers(tagOffset)(kmer) > 0
    }
  }
}

final case class KmerSubtractReducer(k: Int, forwardOnly: Boolean) extends CountReducer {
  //Intersection with this reducer would always remove everything and produce an empty set
  def intersect = false

  //Negate tags (counts)
  override def preprocessSecond(bucket: ReducibleBucket): ReducibleBucket =
    bucket.copy(tags = bucket.tags.map(xs => xs.map(- _)))

  //Since the k-mer was seen in both buckets, with a nonzero count in each, it should not be kept.
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    0

  override def shouldKeep(table: KmerTable, kmer: Tag): Boolean = {
    if (intersect) {
      (table.kmers(tagOffset)(kmer) >> 32) != 0 &&
        table.kmers(tagOffset)(kmer) > 0
    } else {
      table.kmers(tagOffset)(kmer) > 0
    }
  }
}

final case class MinReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    if (count1 < count2) count1 else count2
}

final case class MaxReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    if (count1 > count2) count1 else count2
}

final case class LeftReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    count1
}

final case class RightReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    count2
}

