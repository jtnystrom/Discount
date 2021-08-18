/**
 * Root package for this application.
 */
package object discount {

  /** Type of nucleotide sequences in human-readable form. */
  type NTSeq = String

  /** Type of Sequence IDs (as read from fasta/fastq files) */
  type SequenceID = String

  /** Abundance counts for k-mers. */
  type Abundance = Long
}
