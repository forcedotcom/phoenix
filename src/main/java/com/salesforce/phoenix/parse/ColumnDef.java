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
package com.salesforce.phoenix.parse;

import java.sql.SQLException;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.SchemaUtil;


/**
 * 
 * Represents a column definition during DDL
 *
 * @author jtaylor
 * @since 0.1
 */
public class ColumnDef {
    private final ColumnDefName columnDefName;
    private final PDataType dataType;
    private final boolean isNull;
    private final Integer maxLength;
    private final Integer scale;
    private final boolean isPK;
    private final ColumnModifier columnModifier;
 
    ColumnDef(ColumnDefName columnDefName, String sqlTypeName, boolean isNull, Integer maxLength,
            Integer scale, boolean isPK, ColumnModifier columnModifier) throws SQLException {
        this.columnDefName = columnDefName;
        this.dataType = PDataType.fromSqlTypeName(SchemaUtil.normalizeIdentifier(sqlTypeName));
        this.isNull = isNull;
        if (this.dataType == PDataType.CHAR) {
            if (maxLength == null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MISSING_CHAR_LENGTH)
                    .setColumnName(columnDefName.getColumnName().getName()).build().buildException();
            }
            if (maxLength < 1) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NONPOSITIVE_CHAR_LENGTH)
                    .setColumnName(columnDefName.getColumnName().getName()).build().buildException();
            }
            scale = null;
        } else if (this.dataType == PDataType.VARCHAR) {
            if (maxLength != null && maxLength < 1) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NONPOSITIVE_CHAR_LENGTH)
                    .setColumnName(columnDefName.getColumnName().getName()).build().buildException(); 
            }
            scale = null;
        } else if (this.dataType == PDataType.DECIMAL) {
            maxLength = maxLength == null ? PDataType.MAX_PRECISION : maxLength;
            // for deciaml, 1 <= maxLength <= PDataType.MAX_PRECISION;
            if (maxLength < 1 || maxLength > PDataType.MAX_PRECISION) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.DECIMAL_PRECISION_OUT_OF_RANGE)
                    .setColumnName(columnDefName.getColumnName().getName()).build().buildException();
            }
            // If scale is not specify, it is set to 0. This is the standard as specified in
            // http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1832
            // and 
            // http://docs.oracle.com/javadb/10.6.2.1/ref/rrefsqlj15260.html.
            // Otherwise, if scale is bigger than maxLength, just set it to the maxLength;
            scale = scale == null ? PDataType.DEFAULT_SCALE : scale > maxLength ? maxLength : scale; 
        } else if (this.dataType == PDataType.BINARY) {
            if (maxLength == null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MISSING_BINARY_LENGTH)
                    .setColumnName(columnDefName.getColumnName().getName()).build().buildException();
            }
            if (maxLength < 1) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.NONPOSITIVE_BINARY_LENGTH)
                    .setColumnName(columnDefName.getColumnName().getName()).build().buildException();
            }
            scale = null;
        } else if (this.dataType == PDataType.INTEGER) {
            maxLength = PDataType.INT_PRECISION;
            scale = PDataType.ZERO;
        } else if (this.dataType == PDataType.LONG) {
            maxLength = PDataType.LONG_PRECISION;
            scale = PDataType.ZERO;
        } else {
            // ignore maxLength and scale for other types.
            maxLength = null;
            scale = null;
        }
        this.maxLength = maxLength;
        this.scale = scale;
        this.isPK = isPK;
        this.columnModifier = columnModifier;
    }

    public ColumnDefName getColumnDefName() {
        return columnDefName;
    }

    public PDataType getDataType() {
        return dataType;
    }

    public PDataType getNonPKDataType() {
        switch (dataType) {
        case LONG:
            return PDataType.RAW_LONG;
        default:
            return dataType;
        }
    }

    public boolean isNull() {
        return isNull;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public Integer getScale() {
        return scale;
    }

    public boolean isPK() {
        return isPK;
    }
    
    public ColumnModifier getColumnModifier() {
    	return columnModifier;
    }
}
