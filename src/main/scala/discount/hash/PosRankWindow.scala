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

  def linkPos(before: PositionNode, after: PositionNode) {
    before.nextPos = this
    this.prevPos = before
    this.nextPos = after
    after.prevPos = this
  }
}

trait MotifContainer extends PositionNode {
  def pos: Int
  def motif: Motif
  lazy val rank = motif.features.rank

  /**
   * Drop all nodes before the given position
   * @param from Node to start deleting from
   * @param start The start of the list
   */
  @tailrec
  final def dropUntilPosition(pos: Int) {
    if (this.pos < pos) {
      remove()
      nextPos match {
        case m: MotifContainer => m.dropUntilPosition(pos)
        case _ =>
      }
    }
  }
}

/**
 * End of the list
 */
final class End extends PositionNode

/**
 * Tracks Motifs in a moving window, such that the top priority item can always be obtained efficiently.
 * Items can be removed on the left and insert on the right.
 *
 * Invariants: head of cache is top ranked (minimal rank), and also leftmost position.
 * Priority decreases (i.e. rank increases) monotonically going left to right.
 * Motifs are sorted by position.
 * The minimizer of the current k-length window is always the first motif in the list.
 */
final class PosRankWindow extends PositionNode with Iterable[Motif] {
  val end: End = new End

  //Initial links for an empty list
  nextPos = end
  nextPos.prevPos = this

  //Iterator mostly for testing purposes
  def iterator: Iterator[Motif] = new Iterator[Motif] {
    var cur = nextPos

    override def hasNext: Boolean =
      cur.isInstanceOf[MotifContainer]

    override def next(): Motif = {
      val r = cur.asInstanceOf[MotifContainer].motif
      cur = cur.nextPos
      r
    }
  }

  /**
   * Removes items before the given position
   */
  def dropUntilPosition(pos: Int) {
    nextPos match {
      case mc: MotifContainer => mc.dropUntilPosition(pos)
      case _ =>
    }
  }

  /**
   * Search the list from the end, inserting a new element and possibly dropping a previously inserted suffix
   * in the process.
   * By inserting at the correct position, we maintain the invariant that rank increases monotonically.
   *
   * @param insert
   * @param search
   */
  @tailrec
  private def appendMonotonic(insert: Motif, search: PositionNode): Unit = {
    search.prevPos match {
      case mc: MotifContainer =>
        if (insert.rank < mc.rank) {
          //Keep searching, eventually drop mc
          appendMonotonic(insert, mc)
        } else {
          //Found the right place, insert here and cause subsequent elements to be dropped
          insert.linkPos(mc, end)
        }
      case x =>
        //Inserting into empty list
        insert.linkPos(x, end)
    }
  }

  /**
   * Move the window to the right, dropping elements,
   * and potentially insert a single new element.
   * @param pos New start position of the window
   * @param insertRight Motif to insert, or Motif.Empty for none
   */
  def moveWindowAndInsert(pos: Int, insertRight: Motif): Unit = {
    if (!(insertRight eq Motif.Empty)) {
      appendMonotonic(insertRight, end)
    }
    dropUntilPosition(pos)
  }

  /**
   * Obtain the top ranked element in the list (current minimizer)
   */
  def top: Motif = {
    nextPos match {
      case m: Motif => m
      case _ => Motif.Empty
    }
  }
}