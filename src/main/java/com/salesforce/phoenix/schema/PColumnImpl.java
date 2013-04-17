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

import java.io.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.ByteUtil;


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

    public PColumnImpl() {
    }

    public PColumnImpl(PName name,
                       PName familyName,
                       PDataType dataType,
                       Integer maxLength,
                       Integer scale,
                       boolean nullable,
                       int position) {
        init(name, familyName, dataType, maxLength, scale, nullable, position);
    }

    public PColumnImpl(PColumn column, int position) {
        this(column.getName(), column.getFamilyName(), column.getDataType(), column.getMaxLength(),
                column.getScale(), column.isNullable(), position);
    }

    private void init(PName name,
            PName familyName,
            PDataType dataType,
            Integer maxLength,
            Integer scale,
            boolean nullable,
            int position) {
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
    public String toString() {
        return (familyName == null ? "" : familyName.toString() + QueryConstants.NAME_SEPARATOR) + name.toString();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        byte[] columnNameBytes = Bytes.readByteArray(input);
        PName columnName = new PNameImpl(columnNameBytes);
        byte[] familyNameBytes = Bytes.readByteArray(input);
        PName familyName = familyNameBytes.length == 0 ? null : new PNameImpl(familyNameBytes);
        // TODO: optimize the reading/writing of this b/c it could likely all fit in a single byte or two
        PDataType dataType = PDataType.values()[WritableUtils.readVInt(input)];
        int maxLength = WritableUtils.readVInt(input);
        int scale = WritableUtils.readVInt(input);
        boolean nullable = input.readBoolean();
        int position = WritableUtils.readVInt(input);
        init(columnName, familyName, dataType, maxLength == NO_MAXLENGTH ? null : maxLength,
                scale == NO_SCALE ? null : scale, nullable, position);
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
    }
}