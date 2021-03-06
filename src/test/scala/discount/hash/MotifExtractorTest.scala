package discount.hash

import java.io.{FileInputStream, FileReader}

import org.scalatest.{FunSuite, Matchers}

class MotifExtractorTest extends FunSuite with Matchers {

  test("Read splitting") {
    val m = 2
    val k = 5
    val test = "AATTTACTTTAGTTT"
    val space = MotifSpace.ofLength(m, false)
    val extractor = MotifExtractor(space, k)
    extractor.split(test).toList.map(_._2) should equal(
      List("AATTT", "ATTTA", "TTTACTTT", "CTTTA", "TTTAGTTT"))
  }

  test("Graceful failure") {
    val m = 2
    val k = 5
    val space = MotifSpace.ofLength(m, false)
    val extractor = MotifExtractor(space, k)
    extractor.split("AAAA").toList.isEmpty should equal(true)
    extractor.split("").toList.isEmpty should equal(true)
  }
}