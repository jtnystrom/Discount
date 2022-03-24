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

package com.jnpersson.discount.util

import scala.reflect.ClassTag

object Arrays {

  def fillNew[T <: AnyRef : ClassTag](size: Int, elem: T): Array[T] = {
    val r = new Array[T](size)
    var i = 0
    while (i < size) {
      r(i) = elem
      i += 1
    }
    r
  }

  def fillNewInt(size: Int, elem: Int): Array[Int] = {
    val r = new Array[Int](size)
    var i = 0
    while (i < size) {
      r(i) = elem
      i += 1
    }
    r
  }
}
