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
package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.expression.BaseTerminalExpression;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.parse.SequenceValueParseNode;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PName;
import com.salesforce.phoenix.schema.SequenceKey;
import com.salesforce.phoenix.schema.tuple.Tuple;

public class SequenceManager {
    private final PhoenixStatement statement;
    private int[] sequencePosition;
    private long[] srcSequenceValues;
    private long[] dstSequenceValues;
    private SQLException[] sqlExceptions;
    private List<SequenceKey> nextSequences;
    private List<SequenceKey> currentSequences;
    private Map<SequenceKey,SequenceValueExpression> sequenceMap;
    private BitSet isNextSequence;
    
    public SequenceManager(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    public int getSequenceCount() {
        return sequenceMap == null ? 0 : sequenceMap.size();
    }
    
    private void setSequenceValues() throws SQLException {
        SQLException eTop = null;
        for (int i = 0; i < sqlExceptions.length; i++) {
            SQLException e = sqlExceptions[i];
            if (e != null) {
                if (eTop == null) {
                    eTop = e;
                } else {
                    e.setNextException(eTop.getNextException());
                    eTop.setNextException(e);
                }
            } else {
                dstSequenceValues[sequencePosition[i]] = srcSequenceValues[i];
            }
        }
        if (eTop != null) {
            throw eTop;
        }
    }
    
    public void incrementSequenceValues() throws SQLException {
        if (sequenceMap == null) {
            return;
        }
        Long scn = statement.getConnection().getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        ConnectionQueryServices services = this.statement.getConnection().getQueryServices();
        services.incrementSequenceValues(nextSequences, timestamp, srcSequenceValues, sqlExceptions);
        setSequenceValues();
        int offset = nextSequences.size();
        for (int i = 0; i < currentSequences.size(); i++) {
            dstSequenceValues[sequencePosition[offset+i]] = services.getSequenceValue(currentSequences.get(i), timestamp);
        }
    }

    public SequenceValueExpression newSequenceReference(SequenceValueParseNode node) {
        if (sequenceMap == null) {
            sequenceMap = Maps.newHashMap();
            isNextSequence = new BitSet();
        }
        PName tenantName = statement.getConnection().getTenantId();
        String tenantId = tenantName == null ? null : tenantName.getString();
        TableName tableName = node.getTableName();
        SequenceKey key = new SequenceKey(tenantId, tableName.getSchemaName(), tableName.getTableName());
        SequenceValueExpression expression = sequenceMap.get(key);
        if (expression == null) {
            int index = sequenceMap.size();
            expression = new SequenceValueExpression(index);
            sequenceMap.put(key, expression);
        }
        // If we see a NEXT and a CURRENT, treat the CURRENT just like a NEXT
        if (node.getOp() == SequenceValueParseNode.Op.NEXT_VALUE) {
            isNextSequence.set(expression.getIndex());
        }
           
        return expression;
    }
    
    public void initSequences() throws SQLException {
        if (sequenceMap == null) {
            return;
        }
        int maxSize = sequenceMap.size();
        dstSequenceValues = new long[maxSize];
        sequencePosition = new int[maxSize];
        nextSequences = Lists.newArrayListWithExpectedSize(maxSize);
        currentSequences = Lists.newArrayListWithExpectedSize(maxSize);
        for (Map.Entry<SequenceKey, SequenceValueExpression> entry : sequenceMap.entrySet()) {
            if (isNextSequence.get(entry.getValue().getIndex())) {
                nextSequences.add(entry.getKey());
            } else {
                currentSequences.add(entry.getKey());
            }
        }
        srcSequenceValues = new long[nextSequences.size()];
        sqlExceptions = new SQLException[nextSequences.size()];
        Collections.sort(nextSequences);
        // Create reverse indexes
        for (int i = 0; i < nextSequences.size(); i++) {
            sequencePosition[i] = sequenceMap.get(nextSequences.get(i)).getIndex();
        }
        int offset = nextSequences.size();
        for (int i = 0; i < currentSequences.size(); i++) {
            sequencePosition[i+offset] = sequenceMap.get(currentSequences.get(i)).getIndex();
        }
        ConnectionQueryServices services = this.statement.getConnection().getQueryServices();
        Long scn = statement.getConnection().getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        services.reserveSequenceValues(nextSequences, timestamp, srcSequenceValues, sqlExceptions);
        setSequenceValues();
    }
    
    private class SequenceValueExpression extends BaseTerminalExpression {
        private final int index;
        private final byte[] valueBuffer = new byte[PDataType.LONG.getByteSize()];

        private SequenceValueExpression(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }
        
        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            PDataType.LONG.getCodec().encodeLong(dstSequenceValues[index], valueBuffer, 0);
            ptr.set(valueBuffer);
            return true;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.LONG;
        }
        
        @Override
        public boolean isNullable() {
            return false;
        }
        
        @Override
        public boolean isDeterministic() {
            return false;
        }
        
        @Override
        public boolean isStateless() {
            return true;
        }

    }
}
