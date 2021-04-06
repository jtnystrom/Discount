package discount.hash

import org.scalacheck.{Prop, Properties}
import Prop._

class ShiftScannerProps extends Properties("ShiftScanner") {
  import discount.TestGenerators._

  property("Find all m-mers") = forAll(dnaStrings, ms) { (x, m) =>
    (x.length >= m) ==> {
      val space = MotifSpace.ofLength(m, false)
      val scanner = new ShiftScanner(space)
      scanner.allMatches(x).map(_.pattern).toList == x.sliding(m).toList
    }
  }
}
