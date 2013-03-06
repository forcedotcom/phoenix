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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;

import com.google.common.collect.ImmutableMap;
import com.salesforce.phoenix.schema.PTable;


/**
 * Implementation for PTableStats.
 */
public class PTableStatsImpl implements PTableStats {

    private Map<String, byte[][]> regionGuidePosts;

    public PTableStatsImpl(PTable table) {
        this.regionGuidePosts = new HashMap<String, byte[][]>();
    }

    @Override
    public byte[][] getRegionGuidePost(HRegionInfo region) {
        return regionGuidePosts.get(region.getRegionNameAsString());
    }

    // Only used on the server side by the server thread to update the table.
    public void setRegionGuidePost(HRegionInfo region, byte[][] guidePosts) {
        regionGuidePosts.put(region.getRegionNameAsString(), guidePosts);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Map<String, byte[][]> snapShots = ImmutableMap.co
    }
}
