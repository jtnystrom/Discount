package com.jnpersson.discount

/** Provides classes for hashing k-mers and nucleotide sequences. Hashing is done by identifying minimizers.
 * Hashing all k-mers in a sequence thus corresponds to splitting the sequence into
 * super-mers of length >= k (super k-mers) where all k-mers share the same minimizer.
 */
package object hash {
  /** Type of a compacted hash (minimizer) */
  type BucketId = Long
}
