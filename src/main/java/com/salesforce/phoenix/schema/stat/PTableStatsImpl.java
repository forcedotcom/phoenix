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
package com.salesforce.phoenix.schema.stat;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ImmutableMap;


/**
 * Implementation for PTableStats.
 */
public class PTableStatsImpl implements PTableStats {

    // The map for guide posts should be immutable. We only take the current snapshot from outside
    // method call, store it in this atomic reference to prevent race condition.
    private AtomicReference<Map<String, byte[][]>> regionGuidePosts;

    public PTableStatsImpl() {
        regionGuidePosts = new AtomicReference<Map<String, byte[][]>>();
    }

    public PTableStatsImpl(HashMap<String, byte[][]> stats) {
        this();
        regionGuidePosts.set(ImmutableMap.copyOf(stats));
    }

    @Override
    public byte[][] getRegionGuidePost(HRegionInfo region) {
        return regionGuidePosts.get().get(region.getRegionNameAsString());
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        Map<String, byte[][]> guidePosts = new HashMap<String, byte[][]>();
        int size = WritableUtils.readVInt(input);
        for (int i=0; i<size; i++) {
            String key = WritableUtils.readString(input);
            int valueSize = WritableUtils.readVInt(input);
            byte[][] value = new byte[valueSize][];
            for (int j=0; j<valueSize; j++) {
                value[j] = Bytes.readByteArray(input);
            }
            guidePosts.put(key, value);
        }
        regionGuidePosts.set(ImmutableMap.copyOf(guidePosts));
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Map<String, byte[][]> snapShots = regionGuidePosts.get();
        WritableUtils.writeVInt(output, snapShots.size());
        for (Entry<String, byte[][]> entry : snapShots.entrySet()) {
            WritableUtils.writeString(output, entry.getKey());
            byte[][] value = entry.getValue();
            WritableUtils.writeVInt(output, value.length);
            for (int i=0; i<value.length; i++) {
                Bytes.writeByteArray(output, value[i]);
            }
        }
    }
}
