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
package phoenix.util;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import phoenix.coprocessor.MetaDataProtocol;


/**
 * 
 * Various utilities for scans
 *
 * @author jtaylor
 * @since 0.1
 */
public class ScanUtil {
    private ScanUtil() {
    }

    public static void setTenantId(Scan scan, byte[] tenantId) {
        scan.setAttribute(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
    }
    
    // Use getTenantId and pass in column name to match against
    // in as PSchema attribute. If column name matches in 
    // KeyExpressions, set on scan as attribute
    public static ImmutableBytesWritable getTenantId(Scan scan) {
        // Create Scan with special aggregation column over which to aggregate
        byte[] tenantId = scan.getAttribute(PhoenixRuntime.TENANT_ID_ATTRIB);
        if (tenantId == null) {
            return null;
        }
        return new ImmutableBytesWritable(tenantId);
    }
    
    public static void intersectScanRange(Scan scan, byte[] startKey, byte[] stopKey) {
        byte[] existingStartKey = scan.getStartRow();
        byte[] existingStopKey = scan.getStopRow();
        if (existingStartKey.length > 0) {
            if (startKey.length == 0 || Bytes.compareTo(existingStartKey, startKey) > 0) {
                startKey = existingStartKey;
            }
        }
        if (existingStopKey.length > 0) {
            if (stopKey.length == 0 || Bytes.compareTo(existingStopKey, stopKey) < 0) {
                stopKey = existingStopKey;
            }
        }
        scan.setStartRow(startKey);
        scan.setStopRow(stopKey);
    }
    
    public static void andFilter(Scan scan, Filter andWithFilter) {
        Filter filter = scan.getFilter();
        if (filter == null) {
            scan.setFilter(andWithFilter); 
        } else if (filter instanceof FilterList && ((FilterList)filter).getOperator() == FilterList.Operator.MUST_PASS_ALL) {
            FilterList filterList = (FilterList)filter;
            List<Filter> allFilters = new ArrayList<Filter>(filterList.getFilters().size() + 1);
            allFilters.add(andWithFilter);
            allFilters.addAll(filterList.getFilters());
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,allFilters));
        } else {
            scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,Arrays.asList(filter,andWithFilter)));
        }
    }

    public static void setTimeRange(Scan scan, long ts) {
        try {
            scan.setTimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, ts);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
