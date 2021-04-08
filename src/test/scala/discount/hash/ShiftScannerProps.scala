package discount.hash

import discount.Testing
import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ShiftScannerProps extends AnyFunSuite with ScalaCheckPropertyChecks {
  import discount.TestGenerators._

  test("Find all m-mers") {
    forAll(dnaStrings, ms) { (x, m) =>
      whenever (x.length >= m && m >= 1) {
        val space = Testing.motifSpace(m)
        val scanner = space.scanner
        scanner.allMatches(x).map(_.pattern).toList should equal(x.sliding(m).toList)
      }
    }
  }
}
