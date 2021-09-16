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

import java.io.Serializable;

/**
 * This class is used to store fragments of a long input FASTA 
 * sequence as an array of bytes.
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0  
 */
public class PartialSequence implements Serializable { 
	 
	private static final long serialVersionUID = 6820343484812253133L; 

	private String header;
	private byte[] buffer;
	private int startValue; 
	private int endValue; 
	private int bytesToProcess;

	public String getKey(){
		return header;
	}
	
	public String getValue(){
		return new String(buffer, startValue, (endValue-startValue+1));
	}
	
	public String getValue2(){
		return new String(buffer, startValue, bytesToProcess);
	}
	
	public String toString2() {
		
		if(startValue > 0)
			return ">" + this.getKey() + "\n" + this.getValue2();
		else
			return this.getValue2();

	}

	@Override
	public String toString() {
		return "PartialSequence [header=" + header + 
				", bufferSize=" + (endValue - startValue + 1) +
				", startValue=" + startValue + 
				", endValue=" + endValue + 
				", bytesToProcess=" + bytesToProcess + "]";
	}

	public byte[] getBuffer() {
		return buffer;
	}

	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}

	public int getBytesToProcess() {
		return bytesToProcess;
	}

	public void setBytesToProcess(int bytesToProcess) {
		this.bytesToProcess = bytesToProcess;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public int getStartValue() {
		return startValue;
	}

	public void setStartValue(int startValue) {
		this.startValue = startValue;
	}

	public int getEndValue() {
		return endValue;
	}

	public void setEndValue(int endValue) {
		this.endValue = endValue;
	}

	
}