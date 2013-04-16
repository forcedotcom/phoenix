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

import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Objects;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDatum;

/**
 * 
 * Common base class for column value accessors
 *
 * @author jtaylor
 * @since 0.1
 */
abstract public class ColumnExpression extends BaseTerminalExpression {
    protected PDataType type;
    private Integer byteSize;
    private boolean isNullable;
    private Integer maxLength;
    private Integer scale;
    private ColumnModifier columnModifier;

    public ColumnExpression() {
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (isNullable() ? 1231 : 1237);
        Integer maxLength = this.getByteSize();
        result = prime * result + ((maxLength == null) ? 0 : maxLength.hashCode());
        PDataType type = this.getDataType();
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnExpression other = (ColumnExpression)obj;
        if (this.isNullable() != other.isNullable()) return false;
        if (!Objects.equal(this.getByteSize(),other.getByteSize())) return false;
        if (this.getDataType() != other.getDataType()) return false;
        return true;
    }

    public ColumnExpression(PDatum datum) {
        this.type = datum.getDataType();
        this.isNullable = datum.isNullable();
        if (type.isFixedWidth() && type.getByteSize() == null) {
            this.byteSize = datum.getByteSize();
        }
        this.maxLength = datum.getMaxLength();
        this.scale = datum.getScale();
        this.columnModifier = datum.getColumnModifier();
    }

    @Override
    public boolean isNullable() {
       return isNullable;
    }
    
    @Override
    public PDataType getDataType() {
        return type;
    }
    
    @Override
    public ColumnModifier getColumnModifier() {
    	return columnModifier;
    }

    @Override
    public Integer getByteSize() {
        if (byteSize != null) {
            return byteSize;
        }
        return super.getByteSize();
    }

    @Override
    public Integer getMaxLength() {
        return maxLength;
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // read/write type ordinal and isNullable bit together to save space
        int typeAndNullable = WritableUtils.readVInt(input);
        isNullable = (typeAndNullable & 0x01) != 0;
        type = PDataType.values()[typeAndNullable >>> 1];
        if (type.isFixedWidth() && type.getByteSize() == null) {
            byteSize = WritableUtils.readVInt(input);
        }
        columnModifier = ColumnModifier.fromSystemValue(WritableUtils.readVInt(input));
        
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // read/write type ordinal and isNullable bit together to save space
        int typeAndNullable = (isNullable ? 1 : 0) | (type.ordinal() << 1);
        WritableUtils.writeVInt(output,typeAndNullable);
        if (byteSize != null) {
            WritableUtils.writeVInt(output, byteSize);
        }
        WritableUtils.writeVInt(output, ColumnModifier.toSystemValue(columnModifier));
    }
}
