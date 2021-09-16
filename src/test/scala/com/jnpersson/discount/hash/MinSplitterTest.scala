package com.jnpersson.discount.hash

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should._

class MinSplitterTest extends AnyFunSuite with Matchers {

  test("Read splitting") {
    val m = 2
    val k = 5
    val test = "AATTTACTTTAGTTAC"
    val space = MotifSpace.ofLength(m, false)
    val extractor = MinSplitter(space, k)
    extractor.splitEncode(test).toList.map(_._2.toString) should equal(
      List("AATTT", "ATTTA", "TTTACTTT", "CTTTA", "TTTAGTTA", "GTTAC"))
  }

  test("Graceful failure") {
    val m = 2
    val k = 5
    val space = MotifSpace.ofLength(m, false)
    val extractor = MinSplitter(space, k)
    extractor.splitEncode("AAAA").toList.isEmpty should equal(true)
    extractor.splitEncode("").toList.isEmpty should equal(true)
  }
}