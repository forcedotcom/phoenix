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
package com.salesforce.phoenix.jdbc;

import java.sql.*;

import com.salesforce.phoenix.compile.ColumnProjector;
import com.salesforce.phoenix.compile.RowProjector;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.PDataType;


/**
 * 
 * JDBC ResultSetMetaData implementation of Phoenix.
 * Currently only the following methods are supported:
 * - {@link #getColumnCount()}
 * - {@link #getColumnDisplaySize(int)}
 * - {@link #getColumnLabel(int)} displays alias name if present and column name otherwise
 * - {@link #getColumnName(int)} same as {@link #getColumnLabel(int)}
 * - {@link #isCaseSensitive(int)}
 * - {@link #getColumnType(int)}
 * - {@link #getColumnTypeName(int)}
 * - {@link #getTableName(int)}
 * - {@link #getSchemaName(int)} always returns empty string
 * - {@link #getCatalogName(int)} always returns empty string
 * - {@link #isNullable(int)}
 * - {@link #isSigned(int)}
 * - {@link #isAutoIncrement(int)} always false
 * - {@link #isCurrency(int)} always false
 * - {@link #isDefinitelyWritable(int)} always false
 * - {@link #isReadOnly(int)} always true
 * - {@link #isSearchable(int)} always true
 * 
 * @author jtaylor
 * @since 0.1
 */
public class PhoenixResultSetMetaData implements ResultSetMetaData {
    private static final int MIN_DISPLAY_WIDTH = 3;
    private static final int MAX_DISPLAY_WIDTH = 40;
    private static final int DEFAULT_DISPLAY_WIDTH = 10;
    private final RowProjector rowProjector;
    private final PhoenixConnection connection;
    
    public PhoenixResultSetMetaData(PhoenixConnection connection, RowProjector projector) {
        this.connection = connection;
        this.rowProjector = projector;
    }
    
    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        return type == null ? null : type.getJavaClassName();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return rowProjector.getColumnCount();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        ColumnProjector projector = rowProjector.getColumnProjector(column-1);
        int displaySize = Math.max(projector.getName().length(),MIN_DISPLAY_WIDTH);
        PDataType type = projector.getExpression().getDataType();
        if (type == null) {
            return Math.min(Math.max(displaySize, QueryConstants.NULL_DISPLAY_TEXT.length()), MAX_DISPLAY_WIDTH);
        }
        if (type.isCoercibleTo(PDataType.DATE)) {
            return Math.min(Math.max(displaySize, connection.getDatePattern().length()), MAX_DISPLAY_WIDTH);
        }
        if (projector.getExpression().getByteSize() != null) {
            return Math.min(Math.max(displaySize, projector.getExpression().getByteSize()), MAX_DISPLAY_WIDTH);
        }
        
        return Math.min(Math.max(displaySize, DEFAULT_DISPLAY_WIDTH), MAX_DISPLAY_WIDTH);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).getName();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        // TODO: will return alias if there is one
        return rowProjector.getColumnProjector(column-1).getName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        return type == null ? Types.NULL : type.getResultSetSqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        return type == null ? null : type.getSqlTypeName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        Integer precision = rowProjector.getColumnProjector(column-1).getExpression().getMaxLength();
        return precision == null ? 0 : precision;
    }

    @Override
    public int getScale(int column) throws SQLException {
        Integer scale = rowProjector.getColumnProjector(column-1).getExpression().getScale();
        return scale == null ? 0 : scale;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return ""; // TODO
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).getTableName();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).isCaseSensitive();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return rowProjector.getColumnProjector(column-1).getExpression().isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        PDataType type = rowProjector.getColumnProjector(column-1).getExpression().getDataType();
        if (type == null) {
            return false;
        }
        return type.isCoercibleTo(PDataType.DECIMAL);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }
    
}
