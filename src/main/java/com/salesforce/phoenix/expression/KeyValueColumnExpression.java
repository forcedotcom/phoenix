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
import java.util.Arrays;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.visitor.ExpressionVisitor;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.tuple.Tuple;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 * 
 * Class to access a column value stored in a KeyValue
 *
 * @author jtaylor
 * @since 0.1
 */
public class KeyValueColumnExpression extends ColumnExpression {
    private byte[] cf;
    private byte[] cq;

    public KeyValueColumnExpression() {
    }

    public KeyValueColumnExpression(PColumn column) {
        super(column);
        this.cf = column.getFamilyName().getBytes();
        this.cq = column.getName().getBytes();
    }

    public byte[] getColumnFamily() {
        return cf;
    }

    public byte[] getColumnName() {
        return cq;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(cf);
        result = prime * result + Arrays.hashCode(cq);
        return result;
    }

    // TODO: assumes single table
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        KeyValueColumnExpression other = (KeyValueColumnExpression)obj;
        if (!Arrays.equals(cf, other.cf)) return false;
        if (!Arrays.equals(cq, other.cq)) return false;
        return true;
    }

    @Override
    public String toString() {
        return SchemaUtil.getColumnDisplayName(cf, cq);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        KeyValue keyValue = tuple.getValue(cf, cq);
        if (keyValue != null) {
            ptr.set(keyValue.getBuffer(), keyValue.getValueOffset(), keyValue.getValueLength());
            return true;
        }
        return false;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        cf = Bytes.readByteArray(input);
        cq = Bytes.readByteArray(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        Bytes.writeByteArray(output, cf);
        Bytes.writeByteArray(output, cq);
    }

    @Override
    public final <T> T accept(ExpressionVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
