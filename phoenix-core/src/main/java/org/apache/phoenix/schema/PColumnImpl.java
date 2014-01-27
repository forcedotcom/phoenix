/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.base.Preconditions;


public class PColumnImpl implements PColumn {
    private static final Integer NO_MAXLENGTH = Integer.MIN_VALUE;
    private static final Integer NO_SCALE = Integer.MIN_VALUE;

    private PName name;
    private PName familyName;
    private PDataType dataType;
    private Integer maxLength;
    private Integer scale;
    private boolean nullable;
    private int position;
    private ColumnModifier columnModifier;
    private Integer arraySize;

    public PColumnImpl() {
    }

    public PColumnImpl(PName name,
                       PName familyName,
                       PDataType dataType,
                       Integer maxLength,
                       Integer scale,
                       boolean nullable,
                       int position,
                       ColumnModifier sortOrder, Integer arrSize) {
        init(name, familyName, dataType, maxLength, scale, nullable, position, sortOrder, arrSize);
    }

    public PColumnImpl(PColumn column, int position) {
        this(column.getName(), column.getFamilyName(), column.getDataType(), column.getMaxLength(),
                column.getScale(), column.isNullable(), position, column.getColumnModifier(), column.getArraySize());
    }

    private void init(PName name,
            PName familyName,
            PDataType dataType,
            Integer maxLength,
            Integer scale,
            boolean nullable,
            int position,
            ColumnModifier columnModifier,
            Integer arrSize) {
        this.dataType = dataType;
        if (familyName == null) {
            // Allow nullable columns in PK, but only if they're variable length.
            // Variable length types may be null, since we use a null-byte terminator
            // (which is a disallowed character in variable length types). However,
            // fixed width types do not have a way of representing null.
            // TODO: we may be able to allow this for columns at the end of the PK
            Preconditions.checkArgument(!nullable || !dataType.isFixedWidth(), 
                    "PK columns may not be both fixed width and nullable: " + name.getString());
        }
        this.name = name;
        this.familyName = familyName == null ? null : familyName;
        this.maxLength = maxLength;
        this.scale = scale;
        this.nullable = nullable;
        this.position = position;
        this.columnModifier = columnModifier;
        this.arraySize = arrSize;
    }

    @Override
    public PName getName() {
        return name;
    }

    @Override
    public PName getFamilyName() {
        return familyName;
    }

    @Override
    public PDataType getDataType() {
        return dataType;
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
    public Integer getByteSize() {
        Integer dataTypeMaxLength = dataType.getByteSize();
        return dataTypeMaxLength == null ? dataType.estimateByteSizeFromLength(maxLength)
                : dataTypeMaxLength;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public int getPosition() {
        return position;
    }
    
    @Override
    public ColumnModifier getColumnModifier() {
    	return columnModifier;
    }

    @Override
    public String toString() {
        return (familyName == null ? "" : familyName.toString() + QueryConstants.NAME_SEPARATOR) + name.toString();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        byte[] columnNameBytes = Bytes.readByteArray(input);
        PName columnName = PNameFactory.newName(columnNameBytes);
        byte[] familyNameBytes = Bytes.readByteArray(input);
        PName familyName = familyNameBytes.length == 0 ? null : PNameFactory.newName(familyNameBytes);
        // TODO: optimize the reading/writing of this b/c it could likely all fit in a single byte or two
        PDataType dataType = PDataType.values()[WritableUtils.readVInt(input)];
        int maxLength = WritableUtils.readVInt(input);
        int scale = WritableUtils.readVInt(input);
        boolean nullable = input.readBoolean();
        int position = WritableUtils.readVInt(input);
        ColumnModifier columnModifier = ColumnModifier.fromSystemValue(WritableUtils.readVInt(input));
        int arrSize = WritableUtils.readVInt(input);
        init(columnName, familyName, dataType, maxLength == NO_MAXLENGTH ? null : maxLength,
                scale == NO_SCALE ? null : scale, nullable, position, columnModifier, arrSize == -1 ? null : arrSize);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Bytes.writeByteArray(output, name.getBytes());
        Bytes.writeByteArray(output, familyName == null ? ByteUtil.EMPTY_BYTE_ARRAY : familyName.getBytes());
        WritableUtils.writeVInt(output, dataType.ordinal());
        WritableUtils.writeVInt(output, maxLength == null ? NO_MAXLENGTH : maxLength);
        WritableUtils.writeVInt(output, scale == null ? NO_SCALE : scale);
        output.writeBoolean(nullable);
        WritableUtils.writeVInt(output, position);
        WritableUtils.writeVInt(output, ColumnModifier.toSystemValue(columnModifier));
        WritableUtils.writeVInt(output, arraySize == null ? -1 : arraySize);
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((familyName == null) ? 0 : familyName.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        PColumnImpl other = (PColumnImpl)obj;
        if (familyName == null) {
            if (other.familyName != null) return false;
        } else if (!familyName.equals(other.familyName)) return false;
        if (name == null) {
            if (other.name != null) return false;
        } else if (!name.equals(other.name)) return false;
        return true;
    }

    @Override
    public Integer getArraySize() {
        return arraySize;
    }
}