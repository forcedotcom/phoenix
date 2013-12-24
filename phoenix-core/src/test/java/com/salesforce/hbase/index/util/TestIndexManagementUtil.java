/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.wal.IndexedHLogReader;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALEditCodec;
import org.junit.Test;

public class TestIndexManagementUtil {

  @Test
  public void testUncompressedWal() throws Exception {
    Configuration conf = new Configuration(false);
    // works with WALEditcodec
    conf.set(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
    // clear the codec and set the wal reader
    conf = new Configuration(false);
    conf.set(IndexManagementUtil.HLOG_READER_IMPL_KEY, IndexedHLogReader.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
  }

  /**
   * Compressed WALs are supported when we have the WALEditCodec installed
   * @throws Exception
   */
  @Test
  public void testCompressedWALWithCodec() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    // works with WALEditcodec
    conf.set(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY, IndexedWALEditCodec.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
  }

  /**
   * We cannot support WAL Compression with the IndexedHLogReader
   * @throws Exception
   */
  @Test(expected = IllegalStateException.class)
  public void testCompressedWALWithHLogReader() throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    // works with WALEditcodec
    conf.set(IndexManagementUtil.HLOG_READER_IMPL_KEY, IndexedHLogReader.class.getName());
    IndexManagementUtil.ensureMutableIndexingCorrectlyConfigured(conf);
  }
}