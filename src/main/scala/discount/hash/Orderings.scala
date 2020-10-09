package discount.hash

object Orderings {

  /**
   * Create a MotifSpace that de-prioritizes motifs where either the motif or its reverse
   * complement:
   * 1. Starts with AAA or ACA, or
   * 2. Contains AA anywhere except the beginning
   *
   * This is not the most efficient approach in practice, but useful as a baseline to benchmark against.
   *
   * @param k
   * @return
   */
  def minimizerSignatureSpace(k: Int, w: Int): MotifSpace = {
    val template = MotifSpace.ofLength(w, false)
    val all = template.byPriority
    val withCounts = all.map(mot => (mot, priority(mot)))
    MotifCounter.toSpaceByFrequency(template, withCounts, template.byPriority)
  }

  /**
   * Generate a pseudo-count for each motif.
   * Lower numbers have higher priority.
   * Here we use count "1" to indicate a low priority motif.
   */
  def priority(motif: String): Int = {
    var i = motif.indexOf("AA")
    if (i != -1 && i > 0) {
      return 1
    }

    if (motif.startsWith("AAA") || motif.startsWith("ACA")) {
      return 1
    }
    0
  }
}
