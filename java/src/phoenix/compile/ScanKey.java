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
package phoenix.compile;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.hbase.client.Scan;

import phoenix.query.KeyRange;
import phoenix.schema.RowKeySchema;
import phoenix.schema.ScanKeyOverflowException;
import phoenix.util.ByteUtil;

/**
 * 
 * Meta data for the start/stop key of the scan. Use to generate human readable
 * explain plan.
 *
 * @author jtaylor
 * @since 0.1
 */
public class ScanKey {
    public static final ScanKey EVERYTHING_SCAN_KEY = new ScanKey(KeyRange.UNBOUND_LOWER, true, RowKeySchema.EMPTY_SCHEMA, KeyRange.UNBOUND_UPPER, false, RowKeySchema.EMPTY_SCHEMA, false);
    public static final ScanKey DEGENERATE_SCAN_KEY = new ScanKey(KeyRange.EMPTY_RANGE.getLowerRange(), true, RowKeySchema.EMPTY_SCHEMA, KeyRange.EMPTY_RANGE.getUpperRange(), false, RowKeySchema.EMPTY_SCHEMA, false);
    
    private final byte[] lowerRange;
    private final boolean lowerInclusive;
    private final RowKeySchema lowerSchema;
    private final byte[] upperRange;
    private final boolean upperInclusive;
    private final RowKeySchema upperSchema;
    private final boolean isFullyQualifiedKey;
    
    public ScanKey(byte[] lowerRange, boolean lowerInclusive, RowKeySchema lowerSchema, byte[] upperRange, boolean upperInclusive, RowKeySchema upperSchema, boolean isFullyQualifiedKey) {
        this.lowerRange = lowerRange;
        this.lowerInclusive = lowerInclusive;
        this.lowerSchema = lowerSchema;
        this.upperRange = upperRange;
        this.upperInclusive = upperInclusive;
        this.upperSchema = upperSchema;
        this.isFullyQualifiedKey = isFullyQualifiedKey;
    }

    public RowKeySchema getLowerSchema() {
        return lowerSchema;
    }
    
    public RowKeySchema getUpperSchema() {
        return upperSchema;
    }
    
    public void setScanStartStopKey(Scan scan) {
        if (lowerRange.length > 0) {
            byte[] startKey = lowerRange;
            if (!lowerInclusive) {
                // Adjust start key since hbase is always inclusive for start key
                startKey = ByteUtil.nextKey(startKey);
            }
            scan.setStartRow(startKey);
        }
        if (upperRange.length > 0) {
            byte[] stopKey = upperRange;
            // Adjust stop key since HBase is always non inclusive for stop key
            try {
            	if (upperInclusive) { 
            		stopKey = ByteUtil.nextKey(stopKey);
            	}
            	scan.setStopRow(stopKey);
            } catch (ScanKeyOverflowException e) {
            	// We do not need to set stop key since the stop key already hits
            	// the upper limit. Incrementing it even more would cause an overflow
            	// exception. Since this case is relatively rare, we do not deliberately
            	// check for it, but rather catch the exception here and do nothing.
            }
        }
    }

    public boolean isSingleKey() {
        return isFullyQualifiedKey && ObjectUtils.equals(lowerRange, upperRange) && lowerInclusive && upperInclusive;
    }
    
    public boolean isFullyQualifiedKey() {
        return isFullyQualifiedKey;
    }

    public byte[] getLowerRange() {
        return lowerRange;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public byte[] getUpperRange() {
        return upperRange;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    public boolean isDegenerate() {
        return KeyRange.isDegenerate(lowerRange, upperRange);
    }
}
