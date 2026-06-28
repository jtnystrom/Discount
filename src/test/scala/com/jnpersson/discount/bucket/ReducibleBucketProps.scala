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

import com.jnpersson.discount.Abundance
import com.jnpersson.discount.TestGenerators._
import com.jnpersson.discount.spark.Rule
import com.jnpersson.discount.spark.Rule._
import org.scalatest.funsuite._
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable.{Map => MMap}

class ReducibleBucketProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  val k = 28

  type CountedTable = MMap[List[Long], Abundance]

  implicit class PropsEnhanced(b: ReducibleBucket) {
    def asCountedTable: CountedTable = {
      val r = Reducer.union(k)
      MMap() ++ b.reduceKmers(Reducer.union(k)).iteratorWithTags.
        filter(x => x(r.tagOffset) > 0).map(x => {
        //K-mer data as a list(for deep equality), count tag
        (x.slice(0, x.length - 2).toList, x(x.length - 1))
      })
    }
  }

  /** The expected result of a union type reduction with the given reducer type. */
  def expectedUnion(ab1: Option[Abundance], ab2: Option[Abundance], rule: Rule): Option[Abundance] = {
    rule match {
      case Max =>
        (ab1, ab2) match {
          case (Some(c1), Some(c2)) => Some(List(c1, c2).max)
          case _ => ab1.orElse(ab2)
        }
      case Min =>
        (ab1, ab2) match {
          case (Some(c1), Some(c2)) => Some(List(c1, c2).min)
          case _ => ab1.orElse(ab2)
        }
      case Sum => Some(ab1.getOrElse(0L) + ab2.getOrElse(0L))
      case Left => ab1.orElse(ab2)
      case Right => ab2.orElse(ab1)
      case KmersSubtract => if (ab2.isEmpty) ab1 else None
      case CountersSubtract =>
        val s = ab1.getOrElse(0L) - ab2.getOrElse(0L)
        if (s > 0) Some(s) else None
    }
  }

  /** The expected result of an intersection type reduction with the given reducer type. */
  def expectedIntersection(ab1: Abundance, ab2: Abundance, rule: Rule): Option[Abundance] = {
    rule match {
      case Max => Some(List(ab1, ab2).max)
      case Min => Some(List(ab1, ab2).min)
      case Sum => Some(ab1 + ab2)
      case Left => Some(ab1)
      case Right => Some(ab2)
      case KmersSubtract => ???
      case CountersSubtract =>
        val s = ab1 - ab2
        if (s > 0) Some(s) else None
    }
  }

  def expectedResultsUnion(table1: CountedTable, table2: CountedTable, rule: Rule): CountedTable = {
    val r = MMap[List[Long], Abundance]()
    for {k <- table1.keys ++ table2.keys //May have some redundancy but unimportant
         er <- expectedUnion(table1.get(k), table2.get(k), rule)
         if er != 0}
      r += (k -> er)
    r
  }

  def expectedResultsIntersection(table1: CountedTable, table2: CountedTable, rule: Rule): CountedTable = {
    val r = MMap[List[Long], Abundance]()
    for {k <- table1.keySet.intersect(table2.keySet)
         er <- expectedIntersection(table1(k), table2(k), rule)
         if er != 0 }
      r += (k -> er)
    r
  }

  test("intersect") {
    val rts = List(Min, Max, Sum, CountersSubtract, Left, Right)

    for { rt <- rts } {
      println(s"Intersect $rt")
      forAll(bucketPairWithCommonKmers(k)) { case (b1, b2) =>
        val int = ReducibleBucket.intersectCompact(b1, b2, k, rt)

        int.asCountedTable should equal(
          expectedResultsIntersection(b1.asCountedTable, b2.asCountedTable, rt)
        )
      }
    }
  }

  test ("union") {
    val rts = List(Min, Max, Sum, CountersSubtract, KmersSubtract, Left, Right)

    for { rt <- rts } {
      println(s"Union $rt")
      forAll(bucketPairWithCommonKmers(k)) { case (b1, b2) =>
        val un = ReducibleBucket.unionCompact(Some(b1), Some(b2), k, rt)

        un.asCountedTable should equal(
          expectedResultsUnion(b1.asCountedTable, b2.asCountedTable, rt)
        )
      }
    }
  }
}
