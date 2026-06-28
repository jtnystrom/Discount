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

package com.jnpersson.fastdoop;

import java.io.Serializable;

/**
 * Utility class used to represent as a record a sequence existing 
 * in a FASTQ file.
 * 
 * @author Gianluca Roscigno
 * 
 * @version 1.0
 */
public class QRecord implements Serializable { 
	 
	private static final long serialVersionUID = -7555239567456193078L; 

	private byte[] buffer;
	private int startKey, endKey;
	private int startValue, endValue;
	private int startKey2, endKey2;
	private int startQuality, endQuality;

	public String getKey() {
		return new String(buffer, startKey, (endKey - startKey + 1));
	}

	public String getValue() {
		return new String(buffer, startValue, (endValue - startValue + 1));
	}

	public String getKey2() {
		return new String(buffer, startKey2, (endKey2 - startKey2 + 1));
	}

	public String getQuality() {
		return new String(buffer, startQuality, (endQuality - startQuality + 1));
	}

	@Override
	public String toString() {

		return "@" + this.getKey() + "\n" + this.getValue() + "\n+" + this.getKey2() + "\n" + this.getQuality();

	}

	public byte[] getBuffer() {
		return buffer;
	}

	public int getStartValue() {
		return startValue;
	}

	public int getEndValue() {
		return endValue;
	}

	public void setBuffer(byte[] buffer) {
		this.buffer = buffer;
	}

	public int getStartKey() {
		return startKey;
	}

	public void setStartKey(int startKey) {
		this.startKey = startKey;
	}

	public int getEndKey() {
		return endKey;
	}

	public void setEndKey(int endKey) {
		this.endKey = endKey;
	}

	public void setStartValue(int startValue) {
		this.startValue = startValue;
	}

	public void setEndValue(int endValue) {
		this.endValue = endValue;
	}

	public int getStartKey2() {
		return startKey2;
	}

	public void setStartKey2(int startKey2) {
		this.startKey2 = startKey2;
	}

	public int getEndKey2() {
		return endKey2;
	}

	public void setEndKey2(int endKey2) {
		this.endKey2 = endKey2;
	}

	public int getStartQuality() {
		return startQuality;
	}

	public void setStartQuality(int startQuality) {
		this.startQuality = startQuality;
	}

	public int getEndQuality() {
		return endQuality;
	}

	public void setEndQuality(int endQuality) {
		this.endQuality = endQuality;
	}

}