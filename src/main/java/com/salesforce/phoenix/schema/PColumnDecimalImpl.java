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
import java.sql.*;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.util.ByteUtil;


public class PColumnDecimalImpl extends PColumnImpl {
    private Integer precision;
    private Integer scale;

    public PColumnDecimalImpl(PName name,
            PName familyName,
            PDataType dataType,
            Integer precision,
            Integer scale,
            boolean nullable,
            int position) {
        super(name, familyName, dataType, null, nullable, position);
        this.precision = precision;
        this.scale = scale;
    }

    public PColumnDecimalImpl(PColumn column, int position) {
        this(column.getName(), column.getFamilyName(), column.getDataType(), column.getPrecision(),
                column.getScale(), column.isNullable(), position);
    }

    @Override
    public Integer getScale() {
        return scale;
    }

    @Override
    public Integer getPrecision() {
        return precision;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        byte[] columnNameBytes = Bytes.readByteArray(input);
        PName columnName = new PNameImpl(columnNameBytes);
        byte[] familyNameBytes = Bytes.readByteArray(input);
        PName familyName = familyNameBytes.length == 0 ? null : new PNameImpl(familyNameBytes);
        PDataType dataType = PDataType.values()[WritableUtils.readVInt(input)];
        int precision = WritableUtils.readVInt(input);
        boolean nullable = input.readBoolean();
        int position = WritableUtils.readVInt(input);
        int scale = WritableUtils.readVInt(input);
        init(columnName, familyName, dataType, null, nullable, position);
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Bytes.writeByteArray(output, getName().getBytes());
        Bytes.writeByteArray(output, getFamilyName() == null ? ByteUtil.EMPTY_BYTE_ARRAY : getFamilyName().getBytes());
        WritableUtils.writeVInt(output, getDataType().ordinal());
        WritableUtils.writeVInt(output, precision == null ? PDataType.DEFAULT_PRECISION : precision);
        output.writeBoolean(isNullable());
        WritableUtils.writeVInt(output, getPosition());
        WritableUtils.writeVInt(output, scale == null ? PDataType.DEFAULT_SCALE: scale);
    }

    @Override
    public void prepareInsertStatement(PreparedStatement stmt) throws SQLException {
        stmt.setString(3, getName().getString());
        stmt.setString(4, getFamilyName() == null ? null : getFamilyName().getString());
        stmt.setInt(5, getDataType().getSqlType());
        stmt.setInt(6, isNullable()? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls);
        if (getPrecision() == null) {
            stmt.setNull(7, Types.INTEGER);
        } else {
            stmt.setInt(7, getPrecision());
        }
        stmt.setInt(8, getPosition()+1);
        if (getScale() == null) {
            stmt.setNull(9, Types.INTEGER);
        } else {
            stmt.setInt(9, getScale());
        }
    }
}
