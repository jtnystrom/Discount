package discount.hash

import discount.NTSeq

import scala.collection.mutable.ArrayBuffer
import discount.util.BitRepresentation._
import discount.util.InvalidNucleotideException

/**
 * Bit-shift scanner for fixed width motifs.
 * @param width
 * @param space
 */
final class ShiftScanner(val space: MotifSpace) {

  assert(space.width <= 15)

  def width = space.maxMotifLength

  val mask: Int = {
    var r = 0
    var i = 0
    while (i < width) {
      r = (r << 2) | 3
      i += 1
    }
    r
  }

  val featuresByPriority = {
    space.byPriority.iterator.zipWithIndex.map(p => {
      Features(p._1, p._2, !space.unusedMotifs.contains(p._1))
    }).toArray
  }

  /**
   * Find all matches in the string.
   * Returns an array with the matches in order, or null for positions
   * where no valid matches were found.
   */
  def allMatches(data: NTSeq): ArrayBuffer[Motif] = {
    try {
      val r = new ArrayBuffer[Motif](data.length)
      var pos = 0
      var window: Int = 0
      while ((pos < width - 1) && pos < data.length) {
        window = ((window << 2) | charToTwobit(data.charAt(pos))) & mask
        pos += 1
      }

      while (pos < data.length) {
        window = ((window << 2) | charToTwobit(data.charAt(pos))) & mask
        val priority = space.priorityLookup(window)
        val features = featuresByPriority(priority)
        if (features.valid) {
          val motif = Motif(pos - (width - 1), featuresByPriority(priority))
          r += motif
        } else {
          r += null
        }
        pos += 1
      }
      r
    } catch {
      case ine: InvalidNucleotideException =>
        Console.err.println(s"Unable to parse sequence: '$data' because of character '${ine.invalidChar}' ${ine.invalidChar.toInt}")
        throw ine
    }
  }
}
