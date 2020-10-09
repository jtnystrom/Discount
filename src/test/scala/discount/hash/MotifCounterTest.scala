package discount.hash

import org.scalatest.{FunSuite, Matchers}

class MotifCounterTest extends FunSuite with Matchers {

  test("basic") {
    val space = MotifSpace.ofLength(3, false)
    val reads = Seq("ACTGTT", "TGGTTCCA")
    val counter = MotifCounter(space)
    val scanner = new MotifCountingScanner(space)

    for (r <- reads) {
      scanner.scanRead(counter, r)
    }
    counter.motifsWithCounts(space).toSeq.filter(_._2 > 0) should contain theSameElementsAs(
      List[(String, Int)](
        ("ACT", 1), ("CTG", 1), ("TGT", 1),
        ("GTT", 2), ("TGG", 1), ("GGT", 1),
        ("TTC", 1), ("TCC", 1), ("CCA", 1)
      ))

  }

}
