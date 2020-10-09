package discount.hash

import discount.hash.PosRankWindow.dropUntilPositionRec

import scala.annotation.tailrec

sealed trait PositionNode {
  var prevPos: PositionNode = _  //PosRankWindow or MotifContainer
  var nextPos: PositionNode = _  // End or MotifContainer

  def remove(top: PosRankWindow): Unit = {
    prevPos.nextPos = nextPos
    nextPos.prevPos = prevPos
  }
  def isEnd: Boolean = false
}

trait MotifContainer extends PositionNode {
  def pos: Int
  def motif: Motif
}

final case class End() extends PositionNode {
  override def isEnd = true
}

object PosRankWindow {

  def linkPos(before: PositionNode, middle: PositionNode, after: PositionNode) {
    before.nextPos = middle
    middle.prevPos = before
    middle.nextPos = after
    after.prevPos = middle
  }

  @tailrec
  def dropUntilPositionRec(from: MotifContainer, pos: Int, top: PosRankWindow) {
    if (from.pos < pos) {
      from.remove(top)
      from.nextPos match {
        case m: MotifContainer => dropUntilPositionRec(m, pos, top)
        case _ =>
      }
    }
  }
}

/**
 * The PosRankWindow maintains a doubly linked list.
 * This class is the start motif (lowest pos),
 * and also the main public interface.
 */
final case class PosRankWindow() extends PositionNode {
  nextPos = End()
  nextPos.prevPos = this

  val end: End = nextPos.asInstanceOf[End]

  /**
   * Removes items that can only be parsed
   * before the given sequence position, given the constraints
   * of the given MotifSpace (min permitted start offset, etc)
   */
  def dropUntilPosition(pos: Int) {
    nextPos match {
      case mc: MotifContainer => dropUntilPositionRec(mc, pos, this)
      case _ =>
    }
  }
}

/**
 * Smart cache to support repeated computation of takeByRank(n).
 */
trait TopRankCache {
  def :+= (m: Motif): Unit
  def dropUntilPosition(pos: Int): Unit
  def takeByRank: List[Motif]
}

/**
 * Optimised version of TopRankCache for the case n = 1
 */
final class FastTopRankCache extends TopRankCache {
  /*
   * The cache here is used for the position dimension only, and the rank dimension is ignored.
   *
   * Invariants: head of cache is top ranked, and also leftmost position.
   * Rank decreases (i.e. tagRank increases) monotonically going left to right.
   * Motifs are sorted by position.
   */
  val cache = new PosRankWindow
  var lastRes: List[Motif] = Nil
  var lastResPos: Int = 0
  var lastResRank: Int = Int.MaxValue

  /**
   * Walk the list from the end (lowest priority/high tagRank)
   * ensuring monotonicity.
   */
  @tailrec
  def ensureMonotonic(from: MotifContainer): Unit = {
    from.prevPos match {
      case mc: MotifContainer =>
        if (from.motif.features.tagRank < mc.motif.features.tagRank) {
          mc.remove(cache)
          ensureMonotonic(from)
        }
      case _ =>
    }
  }

  /**
   * Search the list from the end, inserting a new element and possibly
   * dropping a previously inserted suffix in the process.
   * @param insert
   * @param search
   */
  @tailrec
  def appendMonotonic(insert: Motif, search: PositionNode): Unit = {
    search.prevPos match {
      case mc: MotifContainer =>
        val mcr = mc.motif.features.tagRank
        if (insert.motif.features.tagRank < mcr) {
          //Drop mc
          appendMonotonic(insert, mc)
        } else {
          //found the right place, insert here and cause other elements to be dropped
          PosRankWindow.linkPos(mc, insert, cache.end)
        }
      case x =>
        PosRankWindow.linkPos(x, insert, cache.end)
    }
  }

  def :+= (m: Motif): Unit = {
    if (m.features.tagRank < lastResRank) {
      //new item is the highest priority one
      lastRes = Nil
      //wipe pre-existing elements from the cache
      PosRankWindow.linkPos(cache, m, cache.end)
    } else {
      appendMonotonic(m, cache.end)
    }
  }

  def dropUntilPosition(pos: Int): Unit = {
    cache.dropUntilPosition(pos)
    if (pos > lastResPos) {
      lastRes = Nil
    }
  }

  def takeByRank: List[Motif] = {
    if (! (lastRes eq Nil)) {
      lastRes
    } else {
      cache.nextPos match {
        case mc: MotifContainer =>
          lastRes = mc.motif :: Nil
          lastResPos = mc.pos
          lastResRank = mc.motif.features.tagRank
          lastRes
        case _ => Nil
      }
    }
  }
}