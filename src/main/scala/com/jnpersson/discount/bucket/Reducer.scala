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

  /**
   * Apply a binary operation op(from, into) on the tags of the k-mers at these positions,
   * writing the result in the tags of "into", writing the zero value into the tags of "from".
   * This method will only be called on equal k-mers.
   *
   * @param table The table containing k-mers
   * @param into Target k-mer index in the table
   * @param from Source k-mer index in the table
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

  val tagOffset = KmerTable.longsForK(k) + 1

  protected def cappedLongToInt(x: Long): Int =
    if (x > Int.MaxValue) Int.MaxValue else x.toInt

  def reduceEqualKmers(table: KmerTable, into: Int, from: Int): Unit = {
    //Remove any keep flag that may have been set previously
    val count1 = (table.kmers(tagOffset)(from) & Int.MaxValue).toInt
    val count2 = (table.kmers(tagOffset)(into) & Int.MaxValue).toInt

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
  sealed trait Type extends Serializable
  object Sum extends Type
  object Max extends Type
  object Min extends Type
  object First extends Type
  object Second extends Type

  /*
  * Difference (subtraction) reducer that subtracts k-mers in one index from another.
  * As this operation is not commutative, the order of index 1 and 2 are important.
  * Both indexes must previously have been compacted (each k-mer must occur with a nonzero value
  * only once). Both indexes must contain only positive or zero counts before the operation, but the result
  * may contain negative counts.
  *
  * This is a "virtual" reducer that is implemented by negating the counts in one index and then applying Sum.
  */
  object Diff extends Type

  def parseType(typ: String): Type = typ match {
    case "sum" => Sum
    case "max" => Max
    case "min" => Min
    case "first" => First
    case "second" => Second
    case "diff" => Diff
  }

  def unionForK(k: Int, forwardOnly: Boolean, reduction: Type = Sum): Reducer =
    forK(k, forwardOnly, false, reduction)

  def forK(k: Int, forwardOnly: Boolean, intersect: Boolean, reduction: Type): Reducer = {
    reduction match {
      case Sum => SumReducer(k, forwardOnly, intersect)
      case Max => MaxReducer(k, forwardOnly, intersect)
      case Min => MinReducer(k, forwardOnly, intersect)
      case First => FirstReducer(k, forwardOnly, intersect)
      case Second => SecondReducer(k, forwardOnly, intersect)
      case Diff => DiffReducer(k, forwardOnly, intersect)
    }
  }
}

final case class SumReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {

  //Overflow check, since we are generating a new value
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    cappedLongToInt(count1.toLong + count2.toLong)
}

final case class DiffReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {

  //Overflow check, since we are generating a new value
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    cappedLongToInt(count1.toLong - count2.toLong)
}


final case class MinReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    if (count1 < count2) count1 else count2
}

final case class MaxReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    if (count1 > count2) count1 else count2
}

final case class FirstReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    count1
}

final case class SecondReducer(k: Int, forwardOnly: Boolean, intersect: Boolean) extends CountReducer {
  override def reduceCounts(count1: Tag, count2: Tag): Tag =
    count2
}

