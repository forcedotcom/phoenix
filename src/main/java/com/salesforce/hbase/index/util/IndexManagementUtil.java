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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.IndexedHLogReader;
import org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec;
import org.apache.hadoop.hbase.regionserver.wal.WALEditCodec;

import com.google.common.collect.Maps;
import com.salesforce.hbase.index.ValueGetter;
import com.salesforce.hbase.index.builder.IndexBuildingFailureException;
import com.salesforce.hbase.index.covered.data.LazyValueGetter;
import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.hbase.index.scanner.Scanner;

/**
 * Utility class to help manage indexes
 */
public class IndexManagementUtil {

  private IndexManagementUtil() {
    // private ctor for util classes
  }

  private static final Log LOG = LogFactory.getLog(IndexManagementUtil.class);
  public static String HLOG_READER_IMPL_KEY = "hbase.regionserver.hlog.reader.impl";

  public static boolean isWALEditCodecSet(Configuration conf) {
      // check to see if the WALEditCodec is installed
      String codecClass = IndexedWALEditCodec.class.getName();
      if (codecClass.equals(conf.get(WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY, null))) {
        // its installed, and it can handle compression and non-compression cases
        return true;
      }
      return false;
  }
  public static void ensureMutableIndexingCorrectlyConfigured(Configuration conf)
      throws IllegalStateException {

      // check to see if the WALEditCodec is installed
    if (isWALEditCodecSet(conf)) {
        return;
    }

    // otherwise, we have to install the indexedhlogreader, but it cannot have compression
    String codecClass = IndexedWALEditCodec.class.getName();
    String indexLogReaderName = IndexedHLogReader.class.getName();
    if (indexLogReaderName.equals(conf.get(HLOG_READER_IMPL_KEY, indexLogReaderName))) {
      if (conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false)) {
        throw new IllegalStateException("WAL Compression is only supported with " + codecClass
            + ". You can install in hbase-site.xml, under " + WALEditCodec.WAL_EDIT_CODEC_CLASS_KEY);
      }
    } else {
      throw new IllegalStateException(IndexedWALEditCodec.class.getName()
          + " is not installed, but " + indexLogReaderName
          + " hasn't been installed in hbase-site.xml under " + HLOG_READER_IMPL_KEY);
    }

  }

  public static ValueGetter createGetterFromKeyValues(Collection<KeyValue> pendingUpdates) {
    final Map<ReferencingColumn, ImmutableBytesPtr> valueMap =
        Maps.newHashMapWithExpectedSize(pendingUpdates.size());
    for (KeyValue kv : pendingUpdates) {
      // create new pointers to each part of the kv
      ImmutableBytesPtr family =
          new ImmutableBytesPtr(kv.getBuffer(), kv.getFamilyOffset(), kv.getFamilyLength());
      ImmutableBytesPtr qual =
          new ImmutableBytesPtr(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
      ImmutableBytesPtr value =
          new ImmutableBytesPtr(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
      valueMap.put(new ReferencingColumn(family, qual), value);
    }
    return new ValueGetter() {
      @Override
      public ImmutableBytesPtr getLatestValue(ColumnReference ref) throws IOException {
        return valueMap.get(ReferencingColumn.wrap(ref));
      }
    };
  }

  private static class ReferencingColumn {
    ImmutableBytesPtr family;
    ImmutableBytesPtr qual;

    static ReferencingColumn wrap(ColumnReference ref) {
      ImmutableBytesPtr family = new ImmutableBytesPtr(ref.getFamily());
      ImmutableBytesPtr qual = new ImmutableBytesPtr(ref.getQualifier());
      return new ReferencingColumn(family, qual);
    }

    public ReferencingColumn(ImmutableBytesPtr family, ImmutableBytesPtr qual) {
      this.family = family;
      this.qual = qual;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((family == null) ? 0 : family.hashCode());
      result = prime * result + ((qual == null) ? 0 : qual.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      ReferencingColumn other = (ReferencingColumn) obj;
      if (family == null) {
        if (other.family != null) return false;
      } else if (!family.equals(other.family)) return false;
      if (qual == null) {
        if (other.qual != null) return false;
      } else if (!qual.equals(other.qual)) return false;
      return true;
    }
  }

  public static ValueGetter createGetterFromScanner(Scanner scanner, byte[] currentRow) {
    return new LazyValueGetter(scanner, currentRow);
  }

  /** check to see if the kvs in the update  match any of the passed columns. Generally, this is useful to for an index codec to determine if a given update should even be indexed.
  * This assumes that for any index, there are going to small number of columns, versus the number of
  * kvs in any one batch.
  */
  public static boolean updateMatchesColumns(Collection<KeyValue> update,
      List<ColumnReference> columns) {
    // check to see if the kvs in the new update even match any of the columns requested
    // assuming that for any index, there are going to small number of columns, versus the number of
    // kvs in any one batch.
    boolean matches = false;
    outer: for (KeyValue kv : update) {
      for (ColumnReference ref : columns) {
        if (ref.matchesFamily(kv.getFamily()) && ref.matchesQualifier(kv.getQualifier())) {
          matches = true;
          // if a single column matches a single kv, we need to build a whole scanner
          break outer;
        }
      }
    }
    return matches;
  }
  
  /**
   * Check to see if the kvs in the update match any of the passed columns. Generally, this is
   * useful to for an index codec to determine if a given update should even be indexed. This
   * assumes that for any index, there are going to small number of kvs, versus the number of
   * columns in any one batch.
   * <p>
   * This employs the same logic as {@link #updateMatchesColumns(List, List)}, but is flips the
   * iteration logic to search columns before kvs.
   */
  public static boolean columnMatchesUpdate(
      List<ColumnReference> columns, Collection<KeyValue> update) {
    boolean matches = false;
    outer: for (ColumnReference ref : columns) {
      for (KeyValue kv : update) {
        if (ref.matchesFamily(kv.getFamily()) && ref.matchesQualifier(kv.getQualifier())) {
          matches = true;
          // if a single column matches a single kv, we need to build a whole scanner
          break outer;
        }
      }
    }
    return matches;
  }
  
  public static Scan newLocalStateScan(List<? extends Iterable<? extends ColumnReference>> refsArray) {
      Scan s = new Scan();
      s.setRaw(true);
      //add the necessary columns to the scan
      for (Iterable<? extends ColumnReference> refs : refsArray) {
          for(ColumnReference ref : refs){
            s.addFamily(ref.getFamily());
          }
      }
      s.setMaxVersions();
      return s;
  }

  /**
   * Propagate the given failure as a generic {@link IOException}, if it isn't already
   * @param e reason indexing failed. If ,tt>null</tt>, throws a {@link NullPointerException}, which
   *          should unload the coprocessor.
   */
  public static void rethrowIndexingException(Throwable e) throws IOException {
    try {
      throw e;
    } catch (IOException e1) {
      LOG.info("Rethrowing " + e);
      throw e1;
    } catch (Throwable e1) {
      LOG.info("Rethrowing " + e1 + " as a " + IndexBuildingFailureException.class.getSimpleName());
      throw new IndexBuildingFailureException("Failed to build index for unexpected reason!", e1);
    }
  }

  public static void setIfNotSet(Configuration conf, String key, int value) {
    if (conf.get(key) == null) {
      conf.setInt(key, value);
    }
  }
}