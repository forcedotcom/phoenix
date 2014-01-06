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
package com.salesforce.phoenix.schema;

import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;

import com.salesforce.phoenix.util.SchemaUtil;


public class MetaDataSplitPolicy extends ConstantSizeRegionSplitPolicy {

    @Override
    protected byte[] getSplitPoint() {
        byte[] splitPoint = super.getSplitPoint();
        int offset = SchemaUtil.getVarCharLength(splitPoint, 0, splitPoint.length);
        // Split only on Phoenix schema name, so this is ok b/c we won't be splitting
        // in the middle of a Phoenix table.
        if (offset == splitPoint.length) {
            return splitPoint;
        }
//        offset = SchemaUtil.getVarCharLength(splitPoint, offset+1, splitPoint.length-offset-1);
//        // Split only on Phoenix schema and table name, so this is ok b/c we won't be splitting
//        // in the middle of a Phoenix table.
//        if (offset == splitPoint.length) {
//            return splitPoint;
//        }
        // Otherwise, an attempt is being made to split in the middle of a table.
        // Just return a split point at the schema boundary instead
        byte[] newSplitPoint = new byte[offset + 1];
        System.arraycopy(splitPoint, 0, newSplitPoint, 0, offset+1);
        return newSplitPoint;
    }

}
