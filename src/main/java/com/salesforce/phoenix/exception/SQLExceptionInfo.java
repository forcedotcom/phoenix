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
package com.salesforce.phoenix.exception;

import java.sql.SQLException;

import com.salesforce.phoenix.util.SchemaUtil;


/**
 * Object serves as a closure of all coordinate information for SQLException messages.
 * 
 * @author zhuang
 * @since 1.0
 */
public class SQLExceptionInfo {

    /**
     * Constants used in naming exception location.
     */
    public static final String SCHEMA_NAME = "schemaName";
    public static final String TABLE_NAME = "tableName";
    public static final String FAMILY_NAME = "familyName";
    public static final String COLUMN_NAME = "columnName";

    private final Throwable rootCause;
    private final SQLExceptionCode code; // Should always have one.
    private final String message;
    private final String schemaName;
    private final String tableName;
    private final String familyName;
    private final String columnName;

    public static class Builder {

        private Throwable rootCause;
        private SQLExceptionCode code; // Should always have one.
        private String message;
        private String schemaName;
        private String tableName;
        private String familyName;
        private String columnName;

        public Builder(SQLExceptionCode code) {
            this.code = code;
        }

        public Builder setRootCause(Throwable t) {
            this.rootCause = t;
            return this;
        }

        public Builder setMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setFamilyName(String familyName) {
            this.familyName = familyName;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public SQLExceptionInfo build() {
            return new SQLExceptionInfo(this);
        }

        @Override
        public String toString() {
            return code.toString();
        }
    }

    private SQLExceptionInfo(Builder builder) {
        code = builder.code;
        rootCause = builder.rootCause;
        message = builder.message;
        schemaName = builder.schemaName;
        tableName = builder.tableName;
        familyName = builder.familyName;
        columnName = builder.columnName;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(code.toString());
        if (message != null) {
            builder.append(" ").append(message);
        }
        String columnDisplayName = SchemaUtil.getColumnDisplayName(schemaName, tableName, familyName, columnName);
        if (columnName != null) {
            builder.append(" ").append(COLUMN_NAME).append("=").append(columnDisplayName);
        } else if (familyName != null) {
            builder.append(" ").append(FAMILY_NAME).append("=").append(columnDisplayName);
        } else if (tableName != null) {
            builder.append(" ").append(TABLE_NAME).append("=").append(columnDisplayName);
        } else if (schemaName != null) {
            builder.append(" ").append(SCHEMA_NAME).append("=").append(columnDisplayName);
        }
        return builder.toString();
    }

    public SQLException buildException() {
        if (rootCause != null) {
            return new SQLException(toString(), code.getSQLState(), code.getErrorCode(), rootCause);
        } else {
            return new SQLException(toString(), code.getSQLState(), code.getErrorCode(), null);
        }
    }

}
