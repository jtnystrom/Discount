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
