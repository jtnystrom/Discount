package com.jnpersson.discount.hash


/**
 * Object to manage minimizer files that are stored directly on the classpath (e.g. in the same jar)
 */
object BundledMinimizers {
  /**
   * Find the most appropriate bundled minimizers for the given values of m and k,
   * if they exist.
   * @param k
   * @param m
   */
  def getMinimizers(k: Int, m: Int): Option[Array[String]] = {
    val filePaths = (k.to(m + 1, -1)).iterator.map(k => s"/PASHA/minimizers_${k}_${m}.txt")
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
