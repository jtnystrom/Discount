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

package com.jnpersson.discount.hash


/**
 * Object to manage minimizer files that are stored directly on the classpath (e.g. in the same jar)
 */
object BundledMinimizers {
  /**
   * Find the most appropriate bundled minimizers for the given values of m and k,
   * if they exist.
   * @param k k-mer length
   * @param m minimizer length
   */
  def getMinimizers(k: Int, m: Int): Option[Array[String]] = {
    val filePaths = k.to(m + 1, -1).iterator.map(k => s"/PASHA/minimizers_${k}_$m.txt")
    filePaths.flatMap(tryGetMinimizers).buffered.headOption
  }

  private def tryGetMinimizers(location: String): Option[Array[String]] = {
    Option(getClass.getResourceAsStream(location)) match {
      case Some(stream) =>
        println(s"Loading minimizers from $location (on classpath)")
        Some(scala.io.Source.fromInputStream(stream).getLines().toArray)
      case _ => None
    }
  }
}
