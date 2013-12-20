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
package org.apache.hadoop.hbase.regionserver;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Like a {@link KeyValueSkipListSet}, but also exposes useful, atomic methods (e.g.
 * {@link #putIfAbsent(KeyValue)}).
 */
public class IndexKeyValueSkipListSet extends KeyValueSkipListSet {

  // this is annoying that we need to keep this extra pointer around here, but its pretty minimal
  // and means we don't need to change the HBase code.
  private final ConcurrentSkipListMap<KeyValue, KeyValue> delegate;

  /**
   * Create a new {@link IndexKeyValueSkipListSet} based on the passed comparator.
   * @param comparator to use when comparing keyvalues. It is used both to determine sort order as
   *          well as object equality in the map.
   * @return a map that uses the passed comparator
   */
  public static IndexKeyValueSkipListSet create(Comparator<KeyValue> comparator) {
    ConcurrentSkipListMap<KeyValue, KeyValue> delegate =
        new ConcurrentSkipListMap<KeyValue, KeyValue>(comparator);
      return new IndexKeyValueSkipListSet(delegate);
  }

  /**
   * @param delegate map to which to delegate all calls
   */
  public IndexKeyValueSkipListSet(ConcurrentSkipListMap<KeyValue, KeyValue> delegate) {
    super(delegate);
    this.delegate = delegate;
  }

  /**
   * Add the passed {@link KeyValue} to the set, only if one is not already set. This is equivalent
   * to
   * <pre>
   * if (!set.containsKey(key))
   *   return set.put(key);
   * else
   *  return map.set(key);
   * </pre>
   * except that the action is performed atomically.
   * @param kv {@link KeyValue} to add
   * @return the previous value associated with the specified key, or <tt>null</tt> if there was no
   *         previously stored key
   * @throws ClassCastException if the specified key cannot be compared with the keys currently in
   *           the map
   * @throws NullPointerException if the specified key is null
   */
  public KeyValue putIfAbsent(KeyValue kv) {
    return this.delegate.putIfAbsent(kv, kv);
  }
}