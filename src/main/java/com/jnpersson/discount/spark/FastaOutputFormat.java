/*
 * Copyright (c) 2010, The Regents of the University of California, through Lawrence Berkeley
 * National Laboratory (subject to receipt of any required approvals from the U.S. Dept. of Energy).
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 * (1) Redistributions of source code must retain the above copyright notice, this list of conditions and the
 * following disclaimer.
 *
 * (2) Redistributions in binary form must reproduce the above copyright notice, this list of conditions
 * and the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * (3) Neither the name of the University of California, Lawrence Berkeley National Laboratory, U.S. Dept.
 * of Energy, nor the names of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * You are under no obligation whatsoever to provide any bug fixes, patches, or upgrades to the
 * features, functionality or performance of the source code ("Enhancements") to anyone; however,
 * if you choose to make your Enhancements available either publicly, or directly to Lawrence Berkeley
 * National Laboratory, without imposing a separate written license agreement for such Enhancements,
 * then you hereby grant the following license: a  non-exclusive, royalty-free perpetual license to install,
 * use, modify, prepare derivative works, incorporate into other computer software, distribute, and
 * sublicense such enhancements or derivative works thereof, in binary and source code form.
 */

package com.jnpersson.discount.spark;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;


public class FastaOutputFormat<K, V> extends TextOutputFormat<K, V> {
    public static final String fileExtension = ".fasta";

    protected static class FastaRecordWriter<K, V> extends RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";

        private DataOutputStream out;

        public FastaRecordWriter(DataOutputStream out) throws IOException
        {
            this.out = out;
        }


        /**
         * Write the object to the byte stream, handling Text as a special case.
         *
         * @param o
         *          the object to print
         * @throws IOException
         *           if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException
        {
            if (o instanceof Text)
            {
                Text to = (Text)o;
                out.write(to.getBytes(), 0, to.getLength());
            }
            else
            {
                out.write(o.toString().getBytes(utf8));
            }
            out.writeBytes("\n");
        }


        private void writeKey(Object o) throws IOException
        {
            out.writeBytes(">");
            writeObject(o);
        }


        public synchronized void write(K key, V value) throws IOException
        {
            boolean nullKey   = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;

            if (nullKey && nullValue)
            {
                return;
            }

            Object keyObj = key;

            if (nullKey)
            {
                keyObj = "value";
            }

            writeKey(keyObj);

            if (!nullValue)
            {
                writeObject(value);
            }
        }


        public synchronized void close(TaskAttemptContext c) throws IOException
        {
            // even if writeBytes() fails, make sure we close the stream
            out.close();
        }
    }

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException
    {
        Configuration    conf         = job.getConfiguration();
        boolean          isCompressed = getCompressOutput(job);
        CompressionCodec codec        = null;
        String           extension    = fileExtension;

        if (isCompressed)
        {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(job, GzipCodec.class );
            codec     = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
            extension = fileExtension + codec.getDefaultExtension();
        }
        Path       file = getDefaultWorkFile(job, extension);
        FileSystem fs   = file.getFileSystem(conf);
        if (!isCompressed)
        {
            FSDataOutputStream fileOut = fs.create(file, false);
            return(new FastaRecordWriter<K, V>(fileOut));
        }
        else
        {
            FSDataOutputStream fileOut = fs.create(file, false);
            return(new FastaRecordWriter<K, V>(new DataOutputStream
                    (codec.createOutputStream(fileOut))));
        }
    }
}