/*
 * This file is part of Discount. Copyright (c) 2019-2024 Johan Nyström-Persson.
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

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers.{SplitterFormat, StandardFormat}
import com.jnpersson.kmers.util.NTBitArray
import org.apache.spark.sql.SparkSession

import java.util.Properties

class ExtendedFormat extends SplitterFormat[ExtendedTable] {
  val id = "extended"
  val std = new StandardFormat

  /**
   * Write a MinTable's minimizer ordering to a file
   *
   * @param priorities The ordering to write
   * @param props Properties to store flags in
   * @param location Prefix of the location to write to. A suffix will be appended to this name.
   */
  override def write(priorities: ExtendedTable, props: Properties, location: String)(implicit spark: SparkSession): Unit = {
    props.setProperty("canonical", priorities.canonical.toString)
    props.setProperty("extendMethod", if(priorities.withSuffix) "prefixSuffix" else "prefix")
    std.write(priorities.inner, props, location)
  }

  override def read(location: String, props: Properties)(implicit spark: SparkSession): ExtendedTable = {
    val width = props.getProperty("m").toInt
    val canonical = Option(props.getProperty("canonical")).getOrElse("false").toBoolean
    val extendMethod = Option(props.getProperty("extendMethod")).getOrElse("prefixSuffix")
    val withSuffix = extendMethod match {
      case "prefixSuffix" => true
      case _ => false
    }
    ExtendedTable(std.read(location, props), width, canonical, withSuffix)
  }
}

/** An extended minimizer source which expands a MinTable by adding every possible suffix/prefix. */
final case class Extended(inner: MinimizerSource, n: Int, canonical: Boolean, withSuffix: Boolean) extends MinimizerSource {
  override def load(k: Int, m: Int)(implicit spark: SparkSession): Array[Int] =
    inner.load(k, m)

  override def toSplitter(priorities: MinimizerPriorities, k: Int)(implicit spark: SparkSession): MinSplitter[ExtendedTable] = {
    priorities match {
      case mt: MinTable =>
        MinSplitter(ExtendedTable(mt, n, canonical, withSuffix), k)
      case _ => throw new Exception(s"Unable to make an extended minimizer table from $priorities")
    }
  }
}

/**
 * Hybrid lookup/compute MinimizerPriorities, where minimizers consist of a prefix taken from the table and a computed
 * suffix/prefix. This can give low minimizer density for long minimizers while avoiding the need for a huge table.
 *
 * @param inner The inner minimizer ordering (containing the lookup table)
 * @param width Expanded width > inner.width
 */
final case class ExtendedTable(inner: MinTable, width: Int, canonical: Boolean,
                               withSuffix: Boolean) extends MinimizerPriorities {
  assert(width > inner.width)
  assert (inner.width <= 15)

  //Set all bits except the first 2*inner.width
  private val sufMask: NTBitArray = NTBitArray.fill(-1, width) >>>= (2 * inner.width)

  private val shift = (width - inner.width) * 2

  /** Get the priority of the given minimizer.
   * If not every m-mer is a minimizer, then null indicates an invalid minimizer. */
  override def priorityOf(motifArray: NTBitArray): NTBitArray = {
    //in the priority form, bits 1 ... 2 * inner.width will be the priority from the inner motif table.
    //remaining bits.. 2 * width will be the encoded form of the suffix or prefix.

    val normalized = if (canonical) motifArray.canonical else motifArray.clone()

    val prefix = normalized.data(0) >>> (64 - 2 * inner.width)
    val asPrefix = inner.motifArray(prefix.toInt) //TODO avoid allocating a temporary array here
    val prefixComposite = if (asPrefix != null) {
      //insert the prefix at the right place, first masking out the bits that were there
      (normalized.clone() &= sufMask) |= asPrefix //asPrefix is shorter, but the result will be valid
    } else null

    if (withSuffix) {
      val suffix = normalized.clone() <<= shift
      val asSuffix = inner.priorityLookup((suffix.data(0) >>> (64 - 2 * inner.width)).toInt)
      val suffixComposite = if (asSuffix != -1) {
        //suffix in front as prefix
        (normalized >>>= (2 * inner.width)) |= suffix //can avoid clone since suffix is shorter or equal length
      } else null

      //Pick the lexicographically prior: prefix or suffix version
      if (prefixComposite != null && suffixComposite != null) {
        if (prefixComposite < suffixComposite) prefixComposite else suffixComposite
      } else if (prefixComposite != null) {
        prefixComposite
      } else {
        suffixComposite
      }
    } else {
      prefixComposite
    }
  }

  private val innerWidthMask = -1 >>> (32 - inner.width * 2)
  /** Get a representative minimizer for a given priority.  */
  override def motifFor(priority: NTBitArray): NTBitArray = {
    val innerPriority = inner.byPriority(((priority.clone() >>>= shift) <<= shift).toInt & innerWidthMask)
    NTBitArray.fromLong(innerPriority, width) |= (priority.clone() &= sufMask)
  }

  /** Total number of distinct minimizers in this ordering */
  override def numMinimizers: Option[Long] = {
    //4 ^ (width - inner.width)
    val factor = 1L << ((width - inner.width) * 2)
    inner.numMinimizers.map(_ * factor)
  }

  /** Test whether the wider (more specific) minimizer rank is a refinement of the narrow (more general) rank */
  def isRefinementPriority(narrow: Long, wide: Long): Boolean = {
    ((wide >>> shift) ^ narrow) == 0
  }
}