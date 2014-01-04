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
package com.salesforce.phoenix.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.compile.JoinCompiler.ProjectedPTableWrapper;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.schema.KeyValueSchema;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.SchemaUtil;

public class ScanProjector {    
    public static final byte[] VALUE_COLUMN_FAMILY = Bytes.toBytes("_v");
    public static final byte[] VALUE_COLUMN_QUALIFIER = new byte[0];
    
    private static final String SCAN_PROJECTOR = "scanProjector";
    
    private final KeyValueSchema schema;
    private final Expression[] expressions;
    private ValueBitSet valueSet;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    public ScanProjector(ProjectedPTableWrapper projected) {
    	List<PColumn> columns = projected.getTable().getColumns();
    	expressions = new Expression[columns.size() - projected.getTable().getPKColumns().size()];
    	// we do not count minNullableIndex for we might do later merge.
    	KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
    	int i = 0;
        for (PColumn column : projected.getTable().getColumns()) {
        	if (!SchemaUtil.isPKColumn(column)) {
        		builder.addField(column);
        		expressions[i++] = projected.getSourceExpression(column);
        	}
        }
        schema = builder.build();
        valueSet = ValueBitSet.newInstance(schema);
    }
    
    private ScanProjector(KeyValueSchema schema, Expression[] expressions) {
    	this.schema = schema;
    	this.expressions = expressions;
    	this.valueSet = ValueBitSet.newInstance(schema);
    }
    
    public void setValueBitSet(ValueBitSet bitSet) {
        this.valueSet = bitSet;
    }
    
    public static void serializeProjectorIntoScan(Scan scan, ScanProjector projector) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            projector.schema.write(output);
            int count = projector.expressions.length;
            WritableUtils.writeVInt(output, count);
            for (int i = 0; i < count; i++) {
            	WritableUtils.writeVInt(output, ExpressionType.valueOf(projector.expressions[i]).ordinal());
            	projector.expressions[i].write(output);
            }
            scan.setAttribute(SCAN_PROJECTOR, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    public static ScanProjector deserializeProjectorFromScan(Scan scan) {
        byte[] proj = scan.getAttribute(SCAN_PROJECTOR);
        if (proj == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(proj);
        try {
            DataInputStream input = new DataInputStream(stream);
            KeyValueSchema schema = new KeyValueSchema();
            schema.readFields(input);
            int count = WritableUtils.readVInt(input);
            Expression[] expressions = new Expression[count];
            for (int i = 0; i < count; i++) {
            	int ordinal = WritableUtils.readVInt(input);
            	expressions[i] = ExpressionType.values()[ordinal].newInstance();
            	expressions[i].readFields(input);
            }
            return new ScanProjector(schema, expressions);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static class ProjectedValueTuple implements Tuple {
        private ImmutableBytesWritable keyPtr = new ImmutableBytesWritable();
        private long timestamp;
        private byte[] projectedValue;
        private int bitSetLen;
        private KeyValue keyValue;

        private ProjectedValueTuple(byte[] keyBuffer, int keyOffset, int keyLength, long timestamp, byte[] projectedValue, int bitSetLen) {
            this.keyPtr.set(keyBuffer, keyOffset, keyLength);
            this.timestamp = timestamp;
            this.projectedValue = projectedValue;
            this.bitSetLen = bitSetLen;
        }
        
        public ImmutableBytesWritable getKeyPtr() {
            return keyPtr;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
        
        public byte[] getProjectedValue() {
            return projectedValue;
        }
        
        public int getBitSetLength() {
            return bitSetLen;
        }
        
        @Override
        public void getKey(ImmutableBytesWritable ptr) {
            ptr.set(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength());
        }

        @Override
        public KeyValue getValue(int index) {
            if (index != 0) {
                throw new IndexOutOfBoundsException(Integer.toString(index));
            }
            return getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER);
        }

        @Override
        public KeyValue getValue(byte[] family, byte[] qualifier) {
            if (keyValue == null) {
                keyValue = KeyValueUtil.newKeyValue(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength(), 
                        VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, timestamp, projectedValue, 0, projectedValue.length);
            }
            return keyValue;
        }

        @Override
        public boolean getValue(byte[] family, byte[] qualifier,
                ImmutableBytesWritable ptr) {
            ptr.set(projectedValue);
            return true;
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public int size() {
            return 1;
        }
    }
    
    public ProjectedValueTuple projectResults(Tuple tuple) {
    	byte[] bytesValue = schema.toBytes(tuple, expressions, valueSet, ptr);
    	KeyValue base = tuple.getValue(0);
        return new ProjectedValueTuple(base.getBuffer(), base.getRowOffset(), base.getRowLength(), base.getTimestamp(), bytesValue, valueSet.getEstimatedLength());
    }
    
    public static void decodeProjectedValue(Tuple tuple, ImmutableBytesWritable ptr) throws IOException {
    	boolean b = tuple.getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, ptr);
        if (!b)
            throw new IOException("Trying to decode a non-projected value.");
    }
    
    public static ProjectedValueTuple mergeProjectedValue(ProjectedValueTuple dest, KeyValueSchema destSchema, ValueBitSet destBitSet,
    		Tuple src, KeyValueSchema srcSchema, ValueBitSet srcBitSet, int offset) throws IOException {
    	ImmutableBytesWritable destValue = new ImmutableBytesWritable(dest.getProjectedValue());
    	destBitSet.clear();
    	destBitSet.or(destValue);
    	int origDestBitSetLen = dest.getBitSetLength();
    	ImmutableBytesWritable srcValue = new ImmutableBytesWritable();
    	decodeProjectedValue(src, srcValue);
    	srcBitSet.clear();
    	srcBitSet.or(srcValue);
    	int origSrcBitSetLen = srcBitSet.getEstimatedLength();
    	for (int i = 0; i < srcBitSet.getMaxSetBit(); i++) {
    		if (srcBitSet.get(i)) {
    			destBitSet.set(offset + i);
    		}
    	}
    	int destBitSetLen = destBitSet.getEstimatedLength();
    	byte[] merged = new byte[destValue.getLength() - origDestBitSetLen + srcValue.getLength() - origSrcBitSetLen + destBitSetLen];
    	int o = Bytes.putBytes(merged, 0, destValue.get(), destValue.getOffset(), destValue.getLength() - origDestBitSetLen);
    	o = Bytes.putBytes(merged, o, srcValue.get(), srcValue.getOffset(), srcValue.getLength() - origSrcBitSetLen);
    	destBitSet.toBytes(merged, o);
    	ImmutableBytesWritable keyPtr = dest.getKeyPtr();
        return new ProjectedValueTuple(keyPtr.get(), keyPtr.getOffset(), keyPtr.getLength(), dest.getTimestamp(), merged, destBitSetLen);
    }
}
