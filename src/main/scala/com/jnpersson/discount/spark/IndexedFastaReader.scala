/*
 * This file is part of Discount. Copyright (c) 2021 Johan Nyström-Persson.
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

package com.jnpersson.discount.spark

import com.jnpersson.discount.fastdoop._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

import scala.io.Source

/**
 * FAI (fasta index) record
 * @param id Sequence ID
 * @param length length in bps
 * @param start start position (byte offset in file)
 * @param bpsPerLine bps per line
 * @param bytesPerLine bytes per line
 */
case class FAIRecord(id: String, length: Long, start: Long, bpsPerLine: Int, bytesPerLine: Int) {
  /** byte offset of the final character in this sequence */
  val end = (start + (length / bpsPerLine) * bytesPerLine + (length % bpsPerLine)) - 1
}

/**
 * FASTA file reader that s a faidx (.fai) file to track sequence locations.
 * .fai indexes can be generated by various tools, for example seqkit:
 * https://github.com/shenwei356/seqkit/
 *
 * This reader can read a mix of full and partial sequences. If the sequence is fully contained in this split,
 * it will be read as a single [[PartialSequence]] record. Otherwise, it will be read as multiple records.
 * Partial sequences can be identified and reassembled using their header (corresponding to sequence ID)
 * and seqPosition fields.
 *
 * Partial sequences are read together with (k-1) bps from the next part to ensure that full k-mers can be processed.
 *
 * For efficiency, it is not recommended to use this reader for e.g. short reads, or when the maximum size of a
 * sequence is relatively small.
 * [[ShortReadsRecordReader]] and [[FASTQReadsRecordReader]] from Fastdoop are better suited to that task.
 *
 * Inspired by [[LongReadsRecordReader]] from Fastdoop: https://github.com/umbfer/fastdoop
 *
 * @see [[IndexedFastaFormat]]
 */
class IndexedFastaReader extends RecordReader[Text, PartialSequence] {
  //First byte of this split
  private var startByte = 0L

  //Last byte of this split
  private var endByte = 0L

  private var currKey: Text = null

  private var currValue: PartialSequence = null

  private var myInputSplitBuffer = Array[Byte]()

  private var k = 0

  /**
   * FAIRecords corresponding to sequences that we have yet to read
   */
  private var faiRecords: Iterator[FAIRecord] = Iterator.empty
  private var faiSource: Source = null

  private var sizeBuffer1 = 0
  private var sizeBuffer2 = 0

  /**
   * Read a fai file (such as the one generated by 'seqkit faidx') and return the records
   *
   * @param path Path to the fai file
   * @param job
   * @return True if and only if the file existed and was read
   * @throws IOException
   */
  private def readFastaIndex(path: Path, job: Configuration): Iterator[FAIRecord] = {
    val file = path.getFileSystem(job).open(path)
    faiSource = Source.fromInputStream(file)
    faiSource.getLines().map(line => {
      val spl = line.split("\t")
      FAIRecord(spl(0), spl(1).toLong, spl(2).toLong, spl(3).toInt, spl(4).toInt)
    })
  }

  private def setPartialSequencePosition(record: FAIRecord): Unit = {
    /*
    A FAI record provides "bytes per line" and "bps per line".
    For example, if the latter is 61 but the former is 60, then 1 character per line of text is used for newlines.
    Here we calculate the bp position in the nucleotide sequence that this split will start at.
     */

    val seqStartPosition = startByte - record.start
    val row = seqStartPosition / record.bytesPerLine
    val posInLine = seqStartPosition % record.bytesPerLine

    val pos = if (posInLine >= record.bpsPerLine) {
      /*
      The position must be in the region where newline characters end up, so we consider
      ourselves to be on the next line. The position will apply to the next valid nucleotide character after trimming
      whitespace.
       */
      (row + 1) * record.bpsPerLine
    } else {
      //The position is within the normal sequence
      row * record.bpsPerLine + posInLine
    }
    //Convert from 0-based to 1-based sequence position
    currValue.setSeqPosition(pos + 1)
  }

  override def initialize(genericSplit: InputSplit, context: TaskAttemptContext): Unit = {
    val job = context.getConfiguration

    //Used to ensure we read full k-mers (we aim to read k-1 nucleotides from the next split when needed for a
    //partial sequence)
    k = context.getConfiguration.getInt("k", 10)

    val split = genericSplit.asInstanceOf[FileSplit]
    val path = split.getPath
    startByte = split.getStart
    endByte = startByte + split.getLength - 1
    val inputFile = path.getFileSystem(job).open(path)

    val faiPath = new Path(path.toString + ".fai")
    faiRecords = readFastaIndex(faiPath, job)

    //Keep only those FAI records that correspond to sequences in this split
    faiRecords = faiRecords.
      dropWhile(_.end < startByte).takeWhile(_.start <= endByte)

    val inputSplitSize = split.getLength.toInt
    val additionalBytes = k + 2

    //The entire split is read immediately
    myInputSplitBuffer = new Array[Byte](inputSplitSize + additionalBytes)

    sizeBuffer1 = inputFile.read(startByte, myInputSplitBuffer, 0, inputSplitSize)

    if (sizeBuffer1 <= 0) {
      return
    }

    //Additional characters from the next split
    sizeBuffer2 = inputFile.read(startByte + sizeBuffer1, myInputSplitBuffer, sizeBuffer1, additionalBytes)
    inputFile.close()
  }

  private def safeSetBytesToProcess(kmers: Int): Unit = {
    if (kmers < 0) {
      currValue.setBytesToProcess(0)
    } else {
      currValue.setBytesToProcess(kmers)
    }
  }

  override def nextKeyValue(): Boolean = {
    if (!faiRecords.hasNext || sizeBuffer1 <= 0) return false

    val record = faiRecords.next()

    currKey = new Text(record.id)
    currValue = new PartialSequence
    currValue.setHeader(record.id)
    currValue.setBuffer(myInputSplitBuffer)
    if (record.start >= startByte && record.end <= endByte) {
      //Read the sequence in full
      currValue.setSeqPosition(1)
      currValue.setComplete(true)
      currValue.setStartValue((record.start - startByte).toInt)
      currValue.setEndValue((record.end - startByte).toInt)
      //Number of k-mers (and newlines) in the value
      safeSetBytesToProcess((record.end - record.start + 1 - (k - 1)).toInt)
    } else {
      //Read partial
      if (record.start >= startByte) {
        //Started in this split
        currValue.setSeqPosition(1)
        //Skips the sequence header
        currValue.setStartValue((record.start - startByte).toInt)
      } else {
        //Started previously
        setPartialSequencePosition(record)
        currValue.setStartValue(0)
      }
      if (record.end <= endByte) {
        //Sequence ends in this split
        //Excess newline characters at the end will be automatically trimmed
        currValue.setEndValue((record.end - startByte).toInt)
        safeSetBytesToProcess(currValue.getEndValue - currValue.getStartValue + 1 - (k - 1))
      } else {
        //Sequence reaches into next split
        currValue.setEndValue(sizeBuffer1 + sizeBuffer2 - 1)
        //Number of k-mers (mixed with newlines) in the value
        safeSetBytesToProcess(sizeBuffer1 - currValue.getStartValue)

        //If the extra part in the next split was shorter than k - 1, we have to reduce accordingly
        if (sizeBuffer2 < (k - 1)) currValue.setBytesToProcess(
          currValue.getBytesToProcess - ((k - 1) - sizeBuffer2))
      }
    }

    true
  }

  override def getCurrentKey: Text =
    currKey

  override def getCurrentValue: PartialSequence =
    currValue

  override def getProgress: Float =
    if (faiRecords.hasNext) 0 else 1

  override def close(): Unit = {
    //inputFile has already been closed
    faiSource.close()
  }
}
