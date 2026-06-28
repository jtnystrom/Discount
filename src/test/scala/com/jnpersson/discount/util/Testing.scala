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

package com.jnpersson.discount.util

import com.jnpersson.discount.TestGenerators
import org.scalacheck.{Gen, Shrink}
import org.scalatest.matchers.should.Matchers._

import java.util

object Testing {
  import TestGenerators._

  def tagWidths: Gen[Int] = Gen.choose(0, 3)

  def encodedKmers(k: Int): Gen[Array[Long]] =
    dnaStrings(k, k).map(str => NTBitArray.encode(str).data)

  //Disable shrinking of k-mers
  implicit def shrinkEncodedKmers: Shrink[Array[Long]] = Shrink(_ => Stream.empty)
  //Allow the list itself to shrink, but not the k-mers
  implicit def shrinkEncodedKmerList: Shrink[List[Array[Long]]] = Shrink.shrinkContainer
  implicit def shrinkEncodedKmerArray: Shrink[Array[Array[Long]]] = Shrink.shrinkContainer

  def kmerTags(tagWidth: Int): Gen[Array[Long]] =
    Gen.listOfN(tagWidth, Gen.long).map(_.toArray)

  private def sortedKmerTable(k: Int, tagWidth: Int, size: Int,
                              kmers: List[Array[Long]], tags: List[Array[Long]]): KmerTable = {
    val builder = KmerTable.builder(k, size, tagWidth)
    for {
      (km, tg) <- kmers zip tags
    } {
      builder.addLongs(km)
      builder.addLongs(tg)
    }
    builder.result(true)
  }

  /** Generate k-mer tables with tags */
  def kmerTable(k: Int, tagWidth: Int, size: Int): Gen[KmerTable] =
    kmerTable(k, tagWidth, size, Array())

  /** Generate sorted k-mer tables with tags where a set of given k-mers are guaranteed to be included.
   * The final size will be size + includeKmers.length */
  def kmerTable(k: Int, tagWidth: Int, size: Int, includeKmers: Array[Array[Long]]): Gen[KmerTable] = {
    val fullSize = size + includeKmers.length
    for {
      kmers <- Gen.listOfN(size, encodedKmers(k))
      tags <- Gen.listOfN(fullSize, kmerTags(tagWidth))
      table = sortedKmerTable(k, tagWidth, fullSize, kmers ++ includeKmers, tags)
    } yield table
  }

  implicit class PropsEnhancedTable(t: KmerTable) {
    //to help equality checking - arrays don't have deep equals, but lists do
    def listIterator: Iterator[List[Long]] =
      t.iterator.map(_.toList)

    //as above but for k-mers with nonzero tags
    def presentListIterator: Iterator[List[Long]] = {
      val tag = t.kmerWidth + 1
      t.indexIterator.filter(i => t.kmers(tag)(i) > 0).map(i => t(i).toList)
    }
  }

  /** Search-based tag lookup in a sorted kmer table */
  def tagsForKmer(table: KmerTable, kmer: Array[Long]): Option[Array[Long]] =
    table.indices.find(i => util.Arrays.equals(kmer, table(i))).map(j => table.tagsOnly(j))

  def expectTagsForKmer(table: KmerTable, kmer: Array[Long], expected: Array[Long]): Unit = {
    tagsForKmer(table, kmer) match {
      case Some(found) => found.toList should equal(expected.toList)
      case None => throw new Exception("No tags found for k-mer")
    }
  }
}
