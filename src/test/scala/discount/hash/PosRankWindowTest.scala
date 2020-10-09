package discount.hash

import discount.Testing
import org.scalatest.{FunSuite, Matchers}

//Most tests in this file are sensitive to the order of markers in the all2 space
class PosRankWindowTest extends FunSuite with Matchers {
  import Testing._

  test("position") {
    //TODO repair this test and rewrite for FastTopRankCache

//    val test = ms(Seq(("AC", 3), ("AC", 5), ("GT", 10)))
//    val prl = PosRankWindow(test)
//    prl should contain theSameElementsAs(test)
//
//    prl.dropUntilPosition(2)
//    prl should contain theSameElementsAs(test)
//
//    prl.dropUntilPosition(3)
//    prl should contain theSameElementsAs(test)
//    prl.dropUntilPosition(4)
//    prl should contain theSameElementsAs(test.drop(1))
//    prl.dropUntilPosition(6)
//    prl should contain theSameElementsAs(test.drop(2))
//    prl.dropUntilPosition(11)
//    prl should contain theSameElementsAs(Seq())
//
//    assert(prl.end === End())
  }

}