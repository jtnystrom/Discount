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

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.EOFException;
import java.io.IOException;

class Utils {

    /**
     * From FSUtils in HUDI https://github.com/apache/hudi
     * <p>
     * GCS has a different behavior for detecting EOF during seek().
     *
     * @param inputStream FSDataInputStream
     * @return true if the inputstream or the wrapped one is of type GoogleHadoopFSInputStream
     */
    public static boolean isGCSInputStream(FSDataInputStream inputStream) {
        return inputStream.getClass().getCanonicalName().equals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFSInputStream")
                || inputStream.getWrappedStream().getClass().getCanonicalName()
                .equals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFSInputStream");
    }


    /**
     * From FSUtils in HUDI https://github.com/apache/hudi
     *
     * Handles difference in seek behavior for GCS and non-GCS input stream
     * @param inputStream Input Stream
     * @param pos  Position to seek
     * @throws IOException
     */
    public static void safeSeek(FSDataInputStream inputStream, long pos) throws IOException {
        try {
            inputStream.seek(pos);
        } catch (EOFException e) {
            if (isGCSInputStream(inputStream)) {
                inputStream.seek(pos - 1);
            } else {
                throw e;
            }
        }
    }

    /**
     * Adjust an offset into a buffer such that the offset does not overrun the buffer end.
     * @param buffer
     * @param offset
     * @return
     */
    public static int trimToEnd(byte[] buffer, int offset) {
        return (offset <= buffer.length - 1) ? offset : (buffer.length - 1);
    }
}