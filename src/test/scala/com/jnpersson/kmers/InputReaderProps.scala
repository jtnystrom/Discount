/*
 * This file is part of Slacken. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Slacken is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 *  Slacken is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 * along with Slacken.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.kmers

import com.jnpersson.kmers.input.FileInputs
import com.jnpersson.kmers.minimizer.InputFragment
import org.apache.spark.sql.SparkSession
import org.scalacheck.{Gen, Shrink}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Path => FPath}

/** Test the input reader using synthetic files of the various input formats.
  */
class InputReaderProps extends AnyFunSuite with SparkSessionTestWrapper with ScalaCheckPropertyChecks {
  import TestGenerators._

  implicit val sp: SparkSession = spark

  val k = 35

  def generateFileName(extension: String): FPath =
    Files.createTempFile(null, extension)

  //Read files using InputReader
  def readFiles(files: Seq[String]): FileInputs =
    new FileInputs(files, k)

  //Delete a temporary file
  def removeFile(file: String): Unit = {
    HDFSUtil.deleteRecursive(file)
  }

  def removeSeparators(x: String): String =
    x.replaceAll("[\n\r]+", "")

  //File format generators
  case class SeqRecordFile(records: List[(String, InputFragment)], lineSeparator: String) {
    override def toString: String =
      records.map(_._1).mkString("")

    //Write a file using spark's text writer, using the specified compression
    def write(path: String, compression: String) = {
      import spark.implicits._
      val lines = records.map(_._1).toDS
      lines.coalesce(1). //write as a single partition
        write.
        mode("overwrite").
        option("lineSep", lineSeparator). //the line separator is already inserted
        option("compression", compression).
        text(path)
    }
  }

  //Do not shrink an individual record
  implicit def shrinkPair: Shrink[(InputFragment, String)] = Shrink { _ => Stream.empty }

  val fastqQuality =
    (33 to 126).map(_.toChar)

  def compressionFormats: Gen[SeqTitle] =
    Gen.oneOf(List("none", "gzip", "bzip2"))

  def compExtension(compression: String): String = compression match {
    case "gzip" => ".gz"
    case "bzip2" => ".bz2"
    case _ => ""
  }

  //lines have different length, to simulate a complex fasta file
  //Triples of (file data, expected input fragment, line separator)
  def fastaFileShortSequences(lineSep: String, seqGen: Gen[NTSeq]): Gen[(String, InputFragment)] =
    for {
      lines <- Gen.choose(1, 10)
      dnaSeqs <- Gen.listOfN(lines, seqGen)
      id <- Gen.stringOfN(10, Gen.alphaNumChar)
      sequence = dnaSeqs.mkString("")
      record = s">$id$lineSep" + dnaSeqs.mkString(lineSep)
      fragment = InputFragment(id, 0, sequence, None)
    } yield (record, fragment)

  def fastqFileShortSequences(lineSep: String, seqGen: Gen[NTSeq]): Gen[(String, InputFragment)] =
    for {
      dnaSeq <- seqGen
      id <- Gen.stringOfN(10, Gen.alphaNumChar)
      quality <- Gen.stringOfN(dnaSeq.length, Gen.oneOf(fastqQuality))
      record = s"@$id$lineSep$dnaSeq$lineSep+$lineSep$quality"
      fragment = InputFragment(id, 0, dnaSeq, None)
    } yield (record, fragment)

  def lineSeparators: Gen[String] =
    Gen.oneOf(List("\n", "\r\n", "\r"))

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
    forAll(fileGen, compressionFormats) { case (file, comp) =>
      val loc = generateFileName(extension + compExtension(comp)).toString
      file.write(loc, comp)
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
