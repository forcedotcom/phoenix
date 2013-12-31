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
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.expression.BaseTerminalExpression;
import com.salesforce.phoenix.jdbc.PhoenixStatement;
import com.salesforce.phoenix.parse.NextSequenceValueParseNode;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.tuple.Tuple;

public class SequenceManager {
    private final PhoenixStatement statement;
    private Map<TableName,NextSequenceValueExpression> sequenceMap;
    
    public SequenceManager(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    public int getSequenceCount() {
        return sequenceMap == null ? 0 : sequenceMap.size();
    }
    
    /**
     * Called before getting next row to clear any sequence values that
     * were already allocated, as new sequence values must be generated
     * for the next row.
     */
    public void clearCachedSequenceValues() {
        if (sequenceMap == null) {
            return;
        }
        for (NextSequenceValueExpression expression : sequenceMap.values()) {
            expression.valueBuffer = null;
        }
    }

    public NextSequenceValueExpression newSequenceReference(NextSequenceValueParseNode node) {
        if (sequenceMap == null) {
            sequenceMap = Maps.newHashMap();
        }
        NextSequenceValueExpression expression = sequenceMap.get(node.getTableName());
        if (expression == null) {
            expression = new NextSequenceValueExpression(node);
            sequenceMap.put(node.getTableName(), expression);
        }
        return expression;
    }
    
    public void initSequences() throws SQLException {
        statement.initSequences(sequenceMap.keySet());
    }
    
    private class NextSequenceValueExpression extends BaseTerminalExpression {
        private final NextSequenceValueParseNode node;
        private byte[] valueBuffer;

        private NextSequenceValueExpression(NextSequenceValueParseNode node) {
            this.node = node;
        }
        
        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            if (valueBuffer == null) {
                long value;
                try {
                    value = statement.nextSequenceValue(node.getTableName());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                valueBuffer = new byte[PDataType.LONG.getByteSize()];
                PDataType.LONG.getCodec().encodeLong(value, valueBuffer, 0);
            }
            ptr.set(valueBuffer);
            return true;
        }

        @Override
        public PDataType getDataType() {
            return PDataType.LONG;
        }
        
        @Override
        public boolean isConstant() {
            // Enables use of NEXT VALUE FOR in SELECT expression of aggregate query
            return true;
        }

    }
}
