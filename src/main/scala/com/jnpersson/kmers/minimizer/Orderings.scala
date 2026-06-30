/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers.minimizer

/**
 * Routines for creating minimizer orderings.
 */
object Orderings {

  /**
   * Based on a template space, create a MinTable with a random motif ordering.
   * @param template The template ordering to scramble
   * @return
   */
  def randomOrdering(template: MinTable, mask: Long): MinTable = {
    val seed = mask.toInt
    val reorder = template.byPriority.zipWithIndex.
      sortBy(motifIdx => motifIdx._2 ^ seed).
      map(_._1)
    template.copy(byPriority = reorder)
  }
}
