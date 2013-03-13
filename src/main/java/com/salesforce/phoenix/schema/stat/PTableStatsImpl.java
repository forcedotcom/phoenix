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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ImmutableMap;


/**
 * Implementation for PTableStats.
 */
public class PTableStatsImpl implements PTableStats {

    // The map for guide posts should be immutable. We only take the current snapshot from outside
    // method call and store it.
    private Map<String, byte[][]> regionGuidePosts;

    public PTableStatsImpl() { }

    public PTableStatsImpl(Map<String, byte[][]> stats) {
        regionGuidePosts = ImmutableMap.copyOf(stats);
    }

    @Override
    public byte[][] getRegionGuidePosts(HRegionInfo region) {
        return regionGuidePosts.get(region.getRegionNameAsString());
    }

    @Override
    public void write(DataOutput output) throws IOException {
        if (regionGuidePosts == null) {
            WritableUtils.writeVInt(output, 0);
            return;
        }
        WritableUtils.writeVInt(output, regionGuidePosts.size());
        for (Entry<String, byte[][]> entry : regionGuidePosts.entrySet()) {
            WritableUtils.writeString(output, entry.getKey());
            byte[][] value = entry.getValue();
            WritableUtils.writeVInt(output, value.length);
            for (int i=0; i<value.length; i++) {
                Bytes.writeByteArray(output, value[i]);
            }
        }
    }
}
