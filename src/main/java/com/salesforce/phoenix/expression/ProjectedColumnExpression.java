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
package com.salesforce.phoenix.expression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.join.ScanProjector;
import com.salesforce.phoenix.schema.KeyValueSchema;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.SchemaUtil;

public class ProjectedColumnExpression extends ColumnExpression {
	private KeyValueSchema schema;
	private int position;
	
	public ProjectedColumnExpression() {
	}

	public ProjectedColumnExpression(PColumn column, PTable table) {
		super(column);
		this.schema = buildSchema(table);
		this.position = column.getPosition() - table.getPKColumns().size();
	}
    
    private static KeyValueSchema buildSchema(PTable table) {
    	KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        for (PColumn column : table.getColumns()) {
        	if (!SchemaUtil.isPKColumn(column)) {
        		builder.addField(column);
        	}
        }
        return builder.build();
    }
    
    public KeyValueSchema getSchema() {
    	return schema;
    }
    
    public int getPosition() {
    	return position;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + schema.hashCode();
        result = prime * result + position;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ProjectedColumnExpression other = (ProjectedColumnExpression)obj;
        if (!schema.equals(other.schema)) return false;
        if (position != other.position) return false;
        return true;
    }

    @Override
    public String toString() {
        return "{PROJECTED}[" + position + "]";
    }
	
	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		try {
			ImmutableBytesWritable value = ScanProjector.decodeProjectedValue(tuple);
			ValueBitSet bitSet = ValueBitSet.newInstance(schema);
			bitSet.or(value);
			schema.iterator(value, ptr, position, bitSet);
			Boolean hasValue = schema.next(ptr, position, value.getOffset() + value.getLength(), bitSet);
			if (hasValue == null || !hasValue.booleanValue())
				return false;
		} catch (IOException e) {
			return false;
		}
		
		return true;
	}

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        schema = new KeyValueSchema();
        schema.readFields(input);
        position = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        schema.write(output);
        output.writeInt(position);
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }

}
