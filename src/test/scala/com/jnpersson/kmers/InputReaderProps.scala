/*
 *
 *  * This file is part of Slacken. Copyright (c) 2019-2024 Johan Nyström-Persson.
 *  *
 *  * Slacken is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Slacken is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer.InputFragment
import org.scalacheck.{Gen, Shrink}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Test the input reader using synthetic files of the various input formats.
  */
class InputReaderProps extends AnyFunSuite with SparkSessionTestWrapper with ScalaCheckPropertyChecks {
  import TestGenerators._

  implicit val sp = spark

  val k = 35
  val maxReadLength = 100000

  // Write a new temporary file with content
  def generateFile(content: String, extension: String): String = {
    val loc = Files.createTempFile(null, extension)
    Files.write(loc, content.getBytes())
    loc.toString
  }

  //Read files using InputReader
  def readFiles(files: Seq[String]): Inputs =
    new Inputs(files, k, maxReadLength)

  //Delete a temporary file
  def removeFile(file: String): Unit = {
    new java.io.File(file).delete()
  }

  def removeSeparators(x: String): String =
    x.replaceAll("[\n\r]+", "")

  //File format generators
  case class SeqRecordFile(records: List[(String, InputFragment)], lineSeparator: String) {
    override def toString: String =
      records.map(_._1).mkString("")
  }

  //Do not shrink an individual record
  implicit def shrinkPair: Shrink[(InputFragment, String)] = Shrink { _ => Stream.empty }

  val fastqQuality =
    (33 to 126).map(_.toChar)

  //lines have different length, to simulate a complex fasta file
  //Triples of (file data, expected input fragment, line separator)
  def fastaFileShortSequences(lineSep: String, seqGen: Gen[NTSeq]): Gen[(String, InputFragment)] =
    for {
      lines <- Gen.choose(1, 10)
      dnaSeqs <- Gen.listOfN(lines, seqGen)
      id <- Gen.stringOfN(10, Gen.alphaNumChar)
      sequence = dnaSeqs.mkString("")
      record = s">$id$lineSep" + dnaSeqs.mkString(lineSep) + lineSep
      fragment = InputFragment(id, 0, sequence, None)
    } yield (record, fragment)

  def fastqFileShortSequences(lineSep: String, seqGen: Gen[NTSeq]): Gen[(String, InputFragment)] =
    for {
      dnaSeq <- seqGen
      id <- Gen.stringOfN(10, Gen.alphaNumChar)
      quality <- Gen.stringOfN(dnaSeq.length, Gen.oneOf(fastqQuality))
      record = s"@$id$lineSep$dnaSeq$lineSep+$lineSep$quality$lineSep"
      fragment = InputFragment(id, 0, dnaSeq, None)
    } yield (record, fragment)

  def lineSeparators: Gen[String] =
    Gen.oneOf(List("\n", "\n\r"))

  def fastaFiles(seqGen: Gen[NTSeq]): Gen[SeqRecordFile] =
    for {
      lineSep <- lineSeparators
      n <- Gen.choose(1, 10)
      records <- Gen.listOfN(n, fastaFileShortSequences(lineSep, seqGen))
    } yield SeqRecordFile(records, lineSep)

  def fastqFiles(seqGen: Gen[NTSeq]): Gen[SeqRecordFile] =
    for {
      lineSep <- lineSeparators
      n <- Gen.choose(1, 10)
      records <- Gen.listOfN(n, fastqFileShortSequences(lineSep, seqGen))
    } yield SeqRecordFile(records, lineSep)

  def testFileFormat(fileGen: Gen[SeqRecordFile], extension: String, withAmbiguous: Boolean): Unit = {
    forAll(fileGen) { case file =>
      val loc = generateFile(file.toString, extension)
      val inputs = readFiles(List(loc))
      val fragments = file.records.map(pair => (pair._2.header, pair._2.nucleotides)).sortBy(_._1)
      val got = inputs.getInputFragments(withAmbiguous = withAmbiguous).collect().
        toList.sortBy(_.header).map(r =>
        (r.header, removeSeparators(r.nucleotides))
      )
      got should equal(fragments)
      removeFile(loc)
    }
  }

  test("fasta reads fragment reading") {
    testFileFormat(fastaFiles(dnaStringsMixedCaseWithAmbig(k, 100)), ".fasta", true)
    testFileFormat(fastaFiles(dnaStringsMixedCase(k, 100)), ".fasta", false)
  }

  test("fastq reads fragment reading") {
    testFileFormat(fastqFiles(dnaStringsMixedCaseWithAmbig(k, 200)), ".fastq", true)
    testFileFormat(fastqFiles(dnaStringsMixedCase(k, 200)), ".fastq", false)
  }

}
