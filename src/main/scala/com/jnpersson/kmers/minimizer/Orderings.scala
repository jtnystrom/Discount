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

import com.jnpersson.kmers.util.NTBitArray

/**
 * Routines for creating minimizer orderings.
 */
object Orderings {

  /**
   * Create a MinTable that de-prioritizes motifs where either the motif or its reverse
   * complement:
   * 1. Starts with AAA or ACA, or
   * 2. Contains AA anywhere except the beginning
   *
   * The signature ordering is applied on top of an existing ordering in a template table.
   * The existing ordering in that table will then be partially reordered based on the signature priority of each motif.
   *
   * @return
   */
  def minimizerSignatureTable(template: MinTable): MinTable = {
    val (high, low) = template.motifArrays.map(_.toString).partition(signatureHighPriority)
    template.copy(byPriority = (high ++ low).map(NTBitArray.encode(_).toInt))
  }

  /**
   * Is the given motif a high priority motif in the minimizer signature ordering?
   * @param motif The motif to test
   * @return
   */
  def signatureHighPriority(motif: String): Boolean = {
    val i = motif.indexOf("AA")
    if (i != -1 && i > 0) {
      false
    } else if (motif.startsWith("AAA") || motif.startsWith("ACA")) {
      false
    } else true
  }

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
