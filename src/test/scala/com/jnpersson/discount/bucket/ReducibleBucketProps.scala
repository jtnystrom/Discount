/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
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

import com.jnpersson.discount.TestGenerators._
import com.jnpersson.discount.util.KmerTable
import org.scalatest.funsuite._
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks


class ReducibleBucketProps extends AnyFunSuite with ScalaCheckPropertyChecks {

  test("intersect") {
    val k = 28
    val rt = Reducer.Min

    implicit class PropsEnhanced(b: ReducibleBucket) {
      def asSet: Set[List[Long]] = {
        val r = Reducer.unionForK(k, forwardOnly = false)
        b.reduceKmers(Reducer.unionForK(k, forwardOnly = false)).iteratorWithTags.
          filter(x => x(r.tagOffset) > 0).map(x => {
          //Remove data that doesn't respond to the pure k-mer and convert to a List for equality
          x.slice(0, x.length - 2).toList
        }).toSet
      }
    }

    forAll(reducibleBuckets(k)) { b =>
      val selfInt = ReducibleBucket.intersectCompact(b, b, k, rt)
      selfInt.asSet should equal(b.asSet)
    }

    forAll(reducibleBuckets(k), reducibleBuckets(k), reducibleBuckets(k)) { (b1, common, b2) =>
      //Construct two buckets that have a known (minimum) intersection set
      val b1c = ReducibleBucket(1, b1.supermers ++ common.supermers, b1.tags ++ common.tags)
      val b2c = ReducibleBucket(1, b2.supermers ++ common.supermers, b2.tags ++ common.tags)

      val int = ReducibleBucket.intersectCompact(b1c, b2c, k, rt)
      val intSet = b1c.asSet.intersect(b2c.asSet)

      //The intersection may contain more than the common set
      int.asSet should equal(intSet)

      //The intersection should contain all of the common set
      val commonSet = common.asSet
      commonSet.intersect(intSet) should equal(commonSet)
    }
  }
}
