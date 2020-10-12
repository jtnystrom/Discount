/*
 * This file is part of Discount. Copyright (c) 2020 Johan Nyström-Persson.
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

package discount.hash

import discount.NTSeq

/**
 * A scheme for splitting a DNA sequence into superkmers which can be bucketed by hash values.
 *
 * @tparam H the type of hash values that identify buckets.
 */
trait ReadSplitter[H] {
  def k: Int

  /**
   * Split the read into superkmers overlapping by (k-1) bases.
   * @param read
   * @return
   */
  def split(read: NTSeq): Iterator[(H, NTSeq)]

  /**
   * Convert a hashcode into a compact representation.
   * @param hash
   * @return
   */
  def compact(hash: H): BucketId
}
