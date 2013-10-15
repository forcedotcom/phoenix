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
import org.apache.hadoop.hbase.client.Result;
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
import com.salesforce.phoenix.schema.ValueSchema;
import com.salesforce.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import com.salesforce.phoenix.schema.tuple.ResultTuple;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.SchemaUtil;

public class ScanProjector {    
    public static final byte[] VALUE_COLUMN_FAMILY = Bytes.toBytes("_v");
    public static final byte[] VALUE_COLUMN_QUALIFIER = new byte[0];
    
    public static class ProjectedValue {
    	private final ImmutableBytesWritable bytesValue;
    	private final ImmutableBytesWritable valueBitSet;
    	
    	public ProjectedValue(ImmutableBytesWritable bytesValue, ValueBitSet valueBitSet) {
        	byte[] bitSet = new byte[valueBitSet.getEstimatedLength()];
        	int len = valueBitSet.toBytes(bitSet, 0);
        	this.bytesValue = bytesValue;
        	this.valueBitSet = new ImmutableBytesWritable(bitSet, 0, len);
    	}
    	
    	protected ProjectedValue(ImmutableBytesWritable bytesValue, ImmutableBytesWritable valueBitSet) {
    		this.bytesValue = bytesValue;
    		this.valueBitSet = valueBitSet;
    	}
    	
    	public ImmutableBytesWritable getBytesValue() {
    		return bytesValue;
    	}
    	
    	public ImmutableBytesWritable getValueBitSet() {
    		return valueBitSet;
    	}
    	
    	public ValueBitSet getValueBitSetDeserialized(ValueSchema schema) {
    		ValueBitSet bitSet = ValueBitSet.newInstance(schema);
    		bitSet.or(valueBitSet);
    		return bitSet;
    	}
    }
    
    private static final String SCAN_PROJECTOR = "scanProjector";
    
    private final KeyValueSchema schema;
    private final Expression[] expressions;
    private final ValueBitSet valueSet;
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
    
    public KeyValue projectResults(List<KeyValue> results) {
    	assert(results != null && !results.isEmpty());
    	Tuple tuple = new ResultTuple(new Result(results));
    	byte[] bytesValue = schema.toBytes(tuple, expressions, valueSet, ptr);
    	byte[] bitSet = new byte[valueSet.getEstimatedLength()];
    	int len = valueSet.toBytes(bitSet, 0);
    	ProjectedValue value = new ProjectedValue(new ImmutableBytesWritable(bytesValue, 0, bytesValue.length - len), new ImmutableBytesWritable(bitSet, 0, len));
    	byte[] encoded = encodeProjectedValue(value);
    	KeyValue kv = results.get(0);
        return KeyValueUtil.newKeyValue(kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), 
        		VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER, kv.getTimestamp(), encoded, 0, encoded.length);
    }
    
    public static byte[] encodeProjectedValue(ProjectedValue value) {
    	int bitSetLen = value.getValueBitSet().getLength();
    	int valueLen = value.getBytesValue().getLength();
    	byte[] ret = new byte[Bytes.SIZEOF_SHORT + bitSetLen + Bytes.SIZEOF_INT + valueLen];
    	int offset = Bytes.putShort(ret, 0, (short) bitSetLen);
    	offset = Bytes.putBytes(ret, offset, value.getValueBitSet().get(), value.getValueBitSet().getOffset(), bitSetLen);
    	offset = Bytes.putInt(ret, offset, valueLen);
    	Bytes.putBytes(ret, offset, value.getBytesValue().get(), value.getBytesValue().getOffset(), valueLen);
    	
    	return ret;
    }
    
    public static ProjectedValue decodeProjectedValue(List<KeyValue> results) throws IOException {
    	if (results.size() != 1)
    		throw new IOException("Trying to decode a non-projected value.");
    	
    	return decodeProjectedValue(new ResultTuple(new Result(results)));
    }
    
    public static ProjectedValue decodeProjectedValue(Tuple tuple) throws IOException {
    	KeyValue kv = tuple.getValue(VALUE_COLUMN_FAMILY, VALUE_COLUMN_QUALIFIER);
    	if (kv == null)
    		throw new IOException("Trying to decode a non-projected value.");
    	
    	byte[] buffer = kv.getBuffer();
    	int offset = kv.getValueOffset();
    	int bitSetLen = Bytes.toShort(buffer, offset);
    	offset += Bytes.SIZEOF_SHORT;
    	ImmutableBytesWritable bitSet = new ImmutableBytesWritable(buffer, offset, bitSetLen);
    	offset += bitSetLen;
    	int valueLen = Bytes.toInt(buffer, offset);
    	offset += Bytes.SIZEOF_INT;
    	ImmutableBytesWritable value = new ImmutableBytesWritable(buffer, offset, valueLen);
    	return new ProjectedValue(value, bitSet);
    }
}
