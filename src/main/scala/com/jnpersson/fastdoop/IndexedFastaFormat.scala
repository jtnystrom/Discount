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

package com.jnpersson.fastdoop

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

/**
 * Hadoop input format for FASTA files with an accompanying .fai index file.
 *
 * @see [[IndexedFastaReader]]
 *
 * @author Johan Nyström-Persson
 *
 * @version 1.0
 */
class IndexedFastaFormat extends FileInputFormat[Text, PartialSequence] {
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, PartialSequence] =
    new IndexedFastaReader()
}
