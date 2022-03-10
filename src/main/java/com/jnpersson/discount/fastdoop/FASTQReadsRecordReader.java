/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jnpersson.discount.fastdoop;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.RecordReader;

/**
 * This class reads {@literal <key, value>} pairs from an {@code InputSplit}.
 * The input file is in FASTQ format.
 * A FASTA record has a header line that is the key, the data line, an
 * optional single line header string and a quality line.
 * 
 * Example:
 * {@literal @}SRR034939.184 090406_HWI-EAS68_9096_FC400PR_PE_1_1_10 length=100
 * CCACCTCCTGGGTTCAAGGGGTTCTCTTGCCTCAGCTNNNNNNNNNNNNGGNNNNNNNNNTNNNN
 * +SRR034939.184 090406_HWI-EAS68_9096_FC400PR_PE_1_1_10 length=100
 * HDHFHHHHHHFFAFF6?{@literal <}:{@literal <}HHHHHHHHHEDHHHF##!!!!!!!!!!!!##!!!!!!!!!#!!!!
 * ...
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0  
 * 
 * @see InputSplit
 */

public class FASTQReadsRecordReader extends RecordReader<Text, QRecord> {

	private FSDataInputStream inputFile;

	private long startByte;

	private long fileLength;

	private Text currKey;

	private QRecord currRecord;

	/*
	 * Used to buffer the content of the input split
	 */
	private byte[] myInputSplitBuffer;

	/*
	 * Auxiliary buffer used to store the ending buffer of this input split and
	 * the initial bytes of the next split
	 */
	private byte[] borderBuffer;

	/*
	 * Marks the current position in the input split buffer
	 */
	private int posBuffer;

	/*
	 * Stores the size of the input split buffer
	 */
	private int sizeBuffer;
	/*
	 * True, if we processed the entire input split buffer. False, otherwise
	 */
	private boolean endMyInputSplit = false;

	boolean isLastSplit = false;

	public FASTQReadsRecordReader() {
		super();
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {// Called once at
														// initialization.

		posBuffer = 0;
		Configuration job = context.getConfiguration();
		
		int look_ahead_buffer_size = context.getConfiguration().getInt("look_ahead_buffer_size", 4096);

		/*
		 * We open the file corresponding to the input split and
		 * start processing it
		 */
		FileSplit split = (FileSplit) genericSplit;
		Path path = split.getPath();
		fileLength = path.getFileSystem(job).getContentSummary(path).getLength();
		startByte = split.getStart();
		inputFile = path.getFileSystem(job).open(path);
		Utils.safeSeek(inputFile, startByte);

		currKey = new Text("null");
		currRecord = new QRecord();

		/*
		 * We read the whole content of the split in memory using
		 * myInputSplitBuffer. Plus, we read in the memory the first
		 * KV_BUFFER_SIZE of the next split
		 */
		myInputSplitBuffer = new byte[(int) split.getLength()];
		currRecord.setBuffer(myInputSplitBuffer);

		borderBuffer = new byte[look_ahead_buffer_size];

		sizeBuffer = inputFile.read(startByte, myInputSplitBuffer, 0, myInputSplitBuffer.length);
		Utils.safeSeek(inputFile,startByte + sizeBuffer);

		boolean isEOF = (sizeBuffer < 0 || startByte + sizeBuffer >= fileLength);

		if (isEOF) {
			isLastSplit = true;
			int newLineCount = 0;
			int k = 1;

			while (myInputSplitBuffer[myInputSplitBuffer.length - k++] == '\n')
				newLineCount++;

			byte[] tempBuffer = new byte[(int) split.getLength() - newLineCount];
			System.arraycopy(myInputSplitBuffer, 0, tempBuffer, 0, myInputSplitBuffer.length - newLineCount);
			myInputSplitBuffer = tempBuffer;
		}

		for (int i = 0; i < sizeBuffer; i++) {
			if (myInputSplitBuffer[i] == '@') {
				if (i == 0) {
					posBuffer = i + 1;
					break;
				}
				if (myInputSplitBuffer[i - 1] == '\n') {
					posBuffer = i + 1;
					break;
				}

			}
		}

		/*
		 * We skip the first header of the split
		 */
		int j = posBuffer;

		while (myInputSplitBuffer[j] != '\n') {
			j++;
		}

		if (myInputSplitBuffer[j + 1] == '@')
			posBuffer = j + 2;

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (endMyInputSplit) {
			return false;
		}

		boolean nextsplitKey = false;
		boolean nextsplitValue = false;
		boolean nextsplitQuality = false;
		boolean nextsplitSecondHeader = false;

		currRecord.setStartKey(posBuffer);

		/*
		 * We look for the next short sequence my moving posBuffer until a
		 * newline character is found.
		 * End of split is implicitly managed through
		 * ArrayIndexOutOfBoundsException handling
		 */

		try {
			while (myInputSplitBuffer[posBuffer] != '\n') {
				posBuffer++;
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			/*
			 * If we reached the end of the split while scanning a sequence, we
			 * use nextsplitKey to remember that more characters have to be
			 * fetched from the next split for retrieving the key
			 */
			if (!isLastSplit) {
				endMyInputSplit = true;
				nextsplitKey = true;
			} else {
				return false;
			}
		}

		currRecord.setEndKey(posBuffer - 1);

		if (!endMyInputSplit) {
			/*
			 * Assuming there are more characters from the current split to
			 * process, we move forward the pointer
			 * until the symbol '+' is found
			 *
			 * posBuffer + 1 can potentially overrun the buffer end, since the exception above is not thrown
			 * if the final character of the split is a \n. Check the offset accordingly.
			 */
			currRecord.setStartValue(Utils.trimToEnd(myInputSplitBuffer, posBuffer + 1));

			try {
				posBuffer = posBuffer + 2;

				while (myInputSplitBuffer[posBuffer] != '+') {
					posBuffer++;
				}

				currRecord.setEndValue(posBuffer - 2);
				posBuffer++;

			} catch (ArrayIndexOutOfBoundsException e) {

				if (isLastSplit) {
					return false;
				}

				/*
				 * If we reached the end of the split while scanning a sequence,
				 * we use nextsplitValue to remember that more characters have
				 * to be fetched from the next split for retrieving the value
				 */

				endMyInputSplit = true;
				nextsplitValue = true;
				int c = 0;

				if (posBuffer > myInputSplitBuffer.length) {
					posBuffer = myInputSplitBuffer.length;
				}

				for (int i = posBuffer - 1; i >= 0; i--) {
					if (((char) myInputSplitBuffer[i]) != '\n')
						break;

					c++;
				}

				currRecord.setEndValue(posBuffer - 1 - c);

			}

		}

		if (!endMyInputSplit) {

			//The exception above would not be thrown if the final character of the split is a +.
			//Check the offset accordingly.
			currRecord.setStartKey2(Utils.trimToEnd(myInputSplitBuffer, posBuffer));

			try {

				try {
					while (myInputSplitBuffer[posBuffer] != '\n') {
						posBuffer++;
					}
				} catch (ArrayIndexOutOfBoundsException e) {

					if (isLastSplit) {
						return false;
					}
					/*
					 * If we reached the end of the split while scanning a
					 * sequence,
					 * we use nextsplitQuality to remember that more characters
					 * have
					 * to be fetched from the next split for retrieving (and
					 * discarding)
					 * the quality linevalue
					 */
					endMyInputSplit = true;
					nextsplitSecondHeader = true;
					nextsplitQuality = true;
				}
				currRecord.setEndKey2(posBuffer - 1);

				if (!endMyInputSplit) {

					//The exception above would not be thrown if the final character of the split is a newline.
					//Check the offset accordingly.
					currRecord.setStartQuality(Utils.trimToEnd(myInputSplitBuffer, posBuffer + 1));
					currRecord.setEndQuality(currRecord.getStartQuality() + currRecord.getEndValue() - currRecord.getStartValue());
					posBuffer = (currRecord.getEndQuality() + 3);

					if (myInputSplitBuffer.length <= currRecord.getEndQuality()) {

						currRecord.setEndQuality(myInputSplitBuffer.length - 1);
						posBuffer = (myInputSplitBuffer.length - 1);

						throw new ArrayIndexOutOfBoundsException();
					} else {
						if (posBuffer > (myInputSplitBuffer.length - 1)) {
							endMyInputSplit = true;
							return true;
						}
					}
				}

			} catch (ArrayIndexOutOfBoundsException e) {
				if (isLastSplit) {
					return false;
				}

				endMyInputSplit = true;
				nextsplitQuality = true;
			}

		}

		/*
		 * The end of the split has been reached
		 */
		if (endMyInputSplit) {

			/*
			 * If there is another split after this one and we still need to
			 * retrieve the
			 * key of the current record, we switch to borderbuffer to fetch all
			 * the remaining characters
			 */

			if (nextsplitKey) {
				currRecord.setBuffer(borderBuffer);
				int j = posBuffer - currRecord.getStartKey();
				System.arraycopy(myInputSplitBuffer, currRecord.getStartKey(), borderBuffer, 0, j);

				posBuffer = j;

				currRecord.setStartKey(0);
				nextsplitValue = true;
				
				byte b;

				try {

					while ((b = inputFile.readByte()) != '\n')
							borderBuffer[j++] = b;

				} catch (EOFException e) {
					nextsplitValue = false;
				}

				if (!nextsplitValue) {
					return false;
				}

				currRecord.setEndKey(j - 1);

			}

			/*
			 * If there is another split after this one and we still need to
			 * retrieve the value of the current record, we switch to
			 * borderbuffer to fetch all the remaining characters
			 */
			if (nextsplitValue) {

				if (!nextsplitKey) {

					currRecord.setBuffer(borderBuffer);

					int j = currRecord.getEndKey() + 1 - currRecord.getStartKey();
					System.arraycopy(myInputSplitBuffer, currRecord.getStartKey(), borderBuffer, 0, j);

					currRecord.setStartKey(0);
					currRecord.setEndKey(j - 1);

					int start = currRecord.getStartValue();
					currRecord.setStartValue(j);

					if ((currRecord.getEndValue() + 1 - start) > 0)
						System.arraycopy(myInputSplitBuffer, start, borderBuffer, j, (currRecord.getEndValue() + 1 - start));

					if ((currRecord.getEndValue() - start) < 0) {
						posBuffer = j;
					} else {
						posBuffer = j + currRecord.getEndValue() - start;
					}

					currRecord.setEndValue(posBuffer);

					posBuffer++;

				} else {
					posBuffer = currRecord.getEndKey() + 1;
					currRecord.setStartValue(posBuffer);
				}
				
				byte b;

				try {
					while ((b = inputFile.readByte()) != '+') 
						if (b != '\n')
							borderBuffer[posBuffer++] = b;
					
				} catch (EOFException e) {}

				currRecord.setEndValue(posBuffer - 1);

				posBuffer++;
				currRecord.setStartKey2(posBuffer);

				try {

					while ((b = inputFile.readByte()) != '\n')
						borderBuffer[posBuffer++] = b;
					
					currRecord.setEndKey2(posBuffer - 1);

					currRecord.setStartQuality(posBuffer);

					while ((b = inputFile.readByte()) != '\n')
						borderBuffer[posBuffer++] = b;

				} catch (EOFException e) {
					// End file.
				}

				currRecord.setEndQuality(posBuffer - 1);

			}

			/*
			 * If there is another split after this one and we still need to
			 * retrieve the quality line of the current record, we switch to
			 * borderbuffer to fetch all the remaining characters
			 */
			if (nextsplitQuality) {

				currRecord.setBuffer(borderBuffer);

				// copy key
				int j = currRecord.getEndKey() + 1 - currRecord.getStartKey();
				System.arraycopy(myInputSplitBuffer, currRecord.getStartKey(), borderBuffer, 0, j);

				currRecord.setStartKey(0);
				currRecord.setEndKey(j - 1);

				// copy value
				int v = currRecord.getEndValue() + 1 - currRecord.getStartValue();
				System.arraycopy(myInputSplitBuffer, currRecord.getStartValue(), borderBuffer, j, v);

				currRecord.setStartValue(j);
				currRecord.setEndValue(j + v - 1);
				
				byte b;

				if (nextsplitSecondHeader) {
					int start = currRecord.getStartKey2();
					currRecord.setStartKey2(currRecord.getEndValue() + 1);
					posBuffer = currRecord.getStartKey2();

					if ((currRecord.getEndKey2() + 1 - start) > 0)
						System.arraycopy(myInputSplitBuffer, start, borderBuffer, currRecord.getStartKey2(),
								(currRecord.getEndKey2() + 1 - start));

					posBuffer = currRecord.getStartKey2() + (currRecord.getEndKey2() - start);

					currRecord.setEndKey2(posBuffer);
					posBuffer++;

					while ((b = inputFile.readByte()) != '\n')
						borderBuffer[posBuffer++] = b;

					
					currRecord.setEndKey2(posBuffer - 1);
					currRecord.setStartQuality(posBuffer);

				} else {

					int s = currRecord.getEndKey2() + 1 - currRecord.getStartKey2();
					System.arraycopy(myInputSplitBuffer, currRecord.getStartKey2(), borderBuffer, currRecord.getEndValue() + 1,
							s);
					currRecord.setStartKey2(currRecord.getEndValue() + 1);
					currRecord.setEndKey2(currRecord.getStartKey2() + s - 1);

					int start = currRecord.getStartQuality();
					currRecord.setStartQuality(currRecord.getEndKey2() + 1);
					posBuffer = currRecord.getStartQuality();

					if ((currRecord.getEndQuality() + 1 - start) > 0)
						System.arraycopy(myInputSplitBuffer, start, borderBuffer, currRecord.getStartQuality(),
								(currRecord.getEndQuality() + 1 - start));

					posBuffer = currRecord.getStartQuality() + (currRecord.getEndQuality() - start);

					currRecord.setEndQuality(posBuffer);
					posBuffer++;
				}

				try {

					while ((b = inputFile.readByte()) != '\n')
						borderBuffer[posBuffer++] = b;
					
				} catch (EOFException e) {}

				currRecord.setEndQuality(posBuffer - 1);

			}

		}

		return true;

	}

	@Override
	public void close() throws IOException {

		if (inputFile != null)
			inputFile.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currKey;
	}

	@Override
	public QRecord getCurrentValue() throws IOException, InterruptedException {
		return currRecord;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return sizeBuffer > 0 ? posBuffer / sizeBuffer : 1;

	}

}