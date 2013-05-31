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
package com.salesforce.phoenix.iterate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.salesforce.phoenix.schema.tuple.MultiKeyValueTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.ServerUtil;


public class RegionScannerResultIterator extends BaseResultIterator {
    private final RegionScanner scanner;
    
    public RegionScannerResultIterator(RegionScanner scanner) {
        this.scanner = scanner;
        MultiVersionConsistencyControl.setThreadReadPoint(scanner.getMvccReadPoint());
    }
    
    @Override
    public Tuple next() throws SQLException {
        try {
            // TODO: size
            List<KeyValue> results = new ArrayList<KeyValue>();
            // Results are potentially returned even when the return value of s.next is false
            // since this is an indication of whether or not there are more values after the
            // ones returned
            boolean hasMore = scanner.nextRaw(results, null);
            if (!hasMore && results.isEmpty()) {
                return null;
            }
            // We instantiate a new tuple because in all cases currently we hang on to it (i.e.
            // to compute and hold onto the TopN).
            MultiKeyValueTuple tuple = new MultiKeyValueTuple();
            tuple.setKeyValues(results);
            return tuple;
        } catch (IOException e) {
            throw ServerUtil.parseServerException(e);
        }
    }
}
