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

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.salesforce.phoenix.coprocessor.generated.PTableProtos;
import com.salesforce.phoenix.protobuf.ProtobufUtil;
import com.salesforce.phoenix.query.QueryConstants;


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

    public PColumnImpl() {
    }

    public PColumnImpl(PName name,
                       PName familyName,
                       PDataType dataType,
                       Integer maxLength,
                       Integer scale,
                       boolean nullable,
                       int position,
                       ColumnModifier sortOrder) {
        init(name, familyName, dataType, maxLength, scale, nullable, position, sortOrder);
    }

    public PColumnImpl(PColumn column, int position) {
        this(column.getName(), column.getFamilyName(), column.getDataType(), column.getMaxLength(),
                column.getScale(), column.isNullable(), position, column.getColumnModifier());
    }

    private void init(PName name,
            PName familyName,
            PDataType dataType,
            Integer maxLength,
            Integer scale,
            boolean nullable,
            int position,
            ColumnModifier columnModifier) {
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

  /**
   * Create a PColumn instance from PBed PColumn instance
   * @param column
   */
  public static PColumn createFromProto(PTableProtos.PColumn column) {
    byte[] columnNameBytes = column.getColumnNameBytes().toByteArray();
    PName columnName = PNameFactory.newName(columnNameBytes);
    PName familyName = null;
    if (column.hasFamilyNameBytes()) {
      familyName = PNameFactory.newName(column.getFamilyNameBytes().toByteArray());
    }
    PDataType dataType = PDataType.values()[column.getDataType().ordinal()];
    Integer maxLength = null;
    if (column.hasMaxLength()) {
      maxLength = column.getMaxLength();
    }
    Integer scale = null;
    if (column.hasScale()) {
      scale = column.getScale();
    }
    boolean nullable = column.getNullable();
    int position = column.getPosition();
    ColumnModifier columnModifier = ColumnModifier.fromSystemValue(column.getColumnModifier());

    return new PColumnImpl(columnName, familyName, dataType, maxLength, scale, nullable, position,
        columnModifier);
  }

  public static PTableProtos.PColumn toProto(PColumn column) {
    PTableProtos.PColumn.Builder builder = PTableProtos.PColumn.newBuilder();
    builder.setColumnNameBytes(ByteString.copyFrom(column.getName().getBytes()));
    if (column.getFamilyName() != null) {
      builder.setFamilyNameBytes(ByteString.copyFrom(column.getFamilyName().getBytes()));
    }
    builder.setDataType(ProtobufUtil.toPDataTypeProto(column.getDataType()));
    if (column.getMaxLength() != null) {
      builder.setMaxLength(column.getMaxLength());
    }
    if (column.getScale() != null) {
      builder.setScale(column.getScale());
    }
    builder.setNullable(column.isNullable());
    builder.setPosition(column.getPosition());
    builder.setColumnModifier(ColumnModifier.toSystemValue(column.getColumnModifier()));

    return builder.build();
  }
}