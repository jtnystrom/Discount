/*
 * This file is part of Discount. Copyright (c) 2019-2026 Johan Nyström-Persson.
 *
 *  Discount is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Discount is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers.minimizer

import com.jnpersson.kmers.TestGenerators._
import com.jnpersson.kmers.util.NTBitArray
import org.scalacheck.Prop._
import org.scalacheck.Properties

class MinSplitterProps extends Properties("MinSplitter") {
  import com.jnpersson.kmers.TestGenerators.shrinkMAndK

  property("splitting preserves correct data") =
    forAll(mAndKPairsBalanced) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        (k <= x.size) ==> {
          val extractor = MinSplitter(pri, k)
          val encoded = extractor.splitEncode(x).toList
          val supermers = encoded.map(_.nucleotides.toString)
          val recon = supermers.head + supermers.tail.map(_.substring(k - 1)).mkString("")

          (recon == x) :| "original string can be reconstructed from super-mers" &&
            encoded.forall { case Supermer(_, ntseq, location) =>
              x.substring(location.toInt, location.toInt + ntseq.size) == ntseq.toString
            } :| "positions and lengths of super-mers are correct"
        }
      }
    }

  property("adjacent minimizers are not identical") =
    forAll(mAndKPairsBalanced) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        (k <= x.size) ==> {
          val extractor = MinSplitter(pri, k)
          val encoded = extractor.splitEncode(x).map(_.rank.toList)

          encoded.sliding(2).filter(_.length == 2).forall { pair =>
            pair(0) != pair(1)
          }
        }
      }
    }

  property("extracted minimizers are the minimal m-mers in each super-mer") =
    forAll(mAndKPairsBalanced) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        (k <= x.size) ==> {
          val extractor = MinSplitter(pri, k)
          val scanner = ShiftScanner(pri)
          val regions = extractor.splitEncode(x).toList

          //Checking the minimizer in each region.
          //An improved version of this test would compare not only features but also the position of the motif
          val expected = regions.map(r => scanner.allMatches(r.nucleotides)._2.validBitArrayIterator.min)
          val results = regions.map(r => NTBitArray(r.rank, m))

          (results == expected) :| s"$expected == $results"
        }
      }
    }

  property("too short sequences have no minimizers") =
    forAll(mAndKPairsBalanced) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(0, k - 1)) { (pri, x) =>
        val extractor = MinSplitter(pri, k)
        val regions = extractor.splitEncode(x).toList
        regions.isEmpty
      }
    }

  property("Canonical priorities return the same minimizer for reverse complement") =
    forAll(ms) { m =>
      forAll(minimizerPriorities(m, canonical = true), dnaStrings(m, m)) { (pri, x) =>
        val enc = NTBitArray.encode(x)
        val rc = enc.reverseComplement
        pri.priorityOf(enc) == pri.priorityOf(rc)
      }
    }

  property("Super-mers are invariant under reverse complement") =
    forAll(mAndKPairsBalanced) { case (m, k) =>
      forAll(minimizerPriorities(m, canonical = true), dnaStrings(k)) { (pri, x) =>
        (k <= x.size) ==> {
          val extractor = MinSplitter(pri, k)
          val encoded = NTBitArray.encode(x)
          val regions = extractor.splitRead(encoded, false).toList
          val rcRegions = extractor.splitRead(encoded, true).toList

          regions.map(_.rank.toList) == rcRegions.map(_.rank.toList).reverse
        }
      }
    }

  property("splitRead and superkmerPositions return the same data") =
    forAll(mAndKPairsBalanced) { case (m, k) =>
      forAll(minimizerPriorities(m), dnaStrings(k)) { (pri, x) =>
        (k <= x.size) ==> {
          val encoded = NTBitArray.encode(x)
          val extractor = MinSplitter(pri, k)
          val mins1 = extractor.splitRead(encoded).map(_.rank.toList).toList
          val mins2 = extractor.superkmerPositions(encoded).map(_.rank.toList).toList
          mins1 == mins2
        }
      }
    }

}
