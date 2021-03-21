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

import discount.hash.PosRankWindow.dropUntilPositionRec

import scala.annotation.tailrec

/**
 * A node in a doubly linked list that tracks motifs by position
 */
sealed trait PositionNode extends Serializable {
  var prevPos: PositionNode = _  //PosRankWindow or MotifContainer
  var nextPos: PositionNode = _  // End or MotifContainer

  def remove(): Unit = {
    prevPos.nextPos = nextPos
    nextPos.prevPos = prevPos
  }
  def isEnd: Boolean = false
}

trait MotifContainer extends PositionNode {
  def pos: Int
  def motif: Motif
  lazy val rank = motif.features.rank
}

/**
 * End of the list
 */
final class End extends PositionNode {
  override def isEnd = true
}

object PosRankWindow {

  def linkPos(before: PositionNode, middle: PositionNode, after: PositionNode) {
    before.nextPos = middle
    middle.prevPos = before
    middle.nextPos = after
    after.prevPos = middle
  }

  /**
   * Drop all nodes before the given position
   * @param from Node to start deleting from
   * @param start The start of the list
   */
  @tailrec
  def dropUntilPositionRec(from: MotifContainer, pos: Int) {
    if (from.pos < pos) {
      from.remove()
      from.nextPos match {
        case m: MotifContainer => dropUntilPositionRec(m, pos)
        case _ =>
      }
    }
  }
}

/**
 * Main public interface of the position list
 */
final class PosRankWindow extends PositionNode {
  val end: End = new End

  nextPos = end
  nextPos.prevPos = this


  /**
   * Removes items that can only be parsed
   * before the given sequence position, given the constraints
   * of the given MotifSpace (min permitted start offset, etc)
   */
  def dropUntilPosition(pos: Int) {
    nextPos match {
      case mc: MotifContainer => dropUntilPositionRec(mc, pos)
      case _ =>
    }
  }
}

/**
 * Smart cache to support repeated computation of takeByRank.
 */
trait TopRankCache {
  /**
   * Move the window to the right, dropping elements,
   * and potentially insert a single new element.
   * @param pos New start position of the window
   * @param insertRight Motif to insert, or Motif.Empty for none
   */
  def moveWindowAndInsert(pos: Int, insertRight: Motif): Unit

  /**
   * Obtain the top ranked element(s) in the list
   * @return
   */
  def takeByRank: List[Motif]
}


final class FastTopRankCache extends TopRankCache {
  /*
   * The cache here is used for the position dimension only, and the rank dimension is ignored.
   *
   * Invariants: head of cache is top ranked (minimal rank), and also leftmost position.
   * Rank decreases (i.e. rank increases) monotonically going left to right.
   * Motifs are sorted by position.
   */
  val cache = new PosRankWindow
  var lastRes: List[Motif] = Nil
  var lastResPos: Int = -1
  var lastResRank: Int = Int.MaxValue

  /**
   * Walk the list from the end (lowest priority/high rank)
   * ensuring monotonicity of rank.
   */
  @tailrec
  private def ensureMonotonic(from: MotifContainer): Unit = {
    from.prevPos match {
      case mc: MotifContainer =>
        if (from.motif.features.rank < mc.motif.features.rank) {
          mc.remove()
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
  private def appendMonotonic(insert: Motif, search: PositionNode): Unit = {
    search.prevPos match {
      case mc: MotifContainer =>
        if (insert.rank < mc.rank) {
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

  def moveWindowAndInsert(pos: Int, insertRight: Motif): Unit = {
    if (!(insertRight eq Motif.Empty)) {
      this :+= insertRight
    }
    dropUntilPosition(pos)

    if (lastRes eq Nil) {
      cache.nextPos match {
        case mc: MotifContainer =>
          lastRes = mc.motif :: Nil
          lastResPos = mc.pos
          lastResRank = mc.motif.features.rank
        case _ => Nil
      }
    }
  }

  private def :+= (m: Motif): Unit = {
    if (m.features.rank < lastResRank) {
      //new item is the highest priority one
      lastRes = Nil
      //wipe pre-existing elements from the cache
      PosRankWindow.linkPos(cache, m, cache.end)
    } else {
      appendMonotonic(m, cache.end)
    }
  }

  private def dropUntilPosition(pos: Int): Unit = {
    cache.dropUntilPosition(pos)
    if (pos > lastResPos) {
      lastRes = Nil
    }
  }

  def takeByRank: List[Motif] = lastRes
}