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


/**
 * Object serves as a closure of all coordinate information for SQLException messages.
 * 
 * @author zhuang
 * @since 1.0
 */
public class SQLExceptionLocationInfo {

    /**
     * Constants used in naming exception location.
     */
    public static final String SCHEMA_NAME = "schemaName";
    public static final String TABLE_NAME = "tableName";
    public static final String FAMILY_NAME = "familyName";
    public static final String COLUMN_NAME = "columnName";
    public static final String LINE_NUMBER = "lineNumber";

    private String schemaName;
    private String tableName;
    private String familyName;
    private String columnName;
    private String lineNumber;

    private SQLExceptionLocationInfo() {}

    public SQLExceptionLocationInfo setSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public SQLExceptionLocationInfo setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public SQLExceptionLocationInfo setFamilyName(String familyName) {
        this.familyName = familyName;
        return this;
    }

    public SQLExceptionLocationInfo setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public SQLExceptionLocationInfo setLineNumber(String lineNumber) {
        this.lineNumber = lineNumber;
        return this;
    }

    public static SQLExceptionLocationInfo getNewInfoObject() {
        return new SQLExceptionLocationInfo();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (schemaName != null) {
            builder.append(SCHEMA_NAME).append("=").append(schemaName).append(";");
        }
        if (tableName != null) {
            builder.append(TABLE_NAME).append("=").append(tableName).append(";");
        }
        if (familyName != null) {
            builder.append(FAMILY_NAME).append("=").append(familyName).append(";");
        }
        if (columnName != null) {
            builder.append(COLUMN_NAME).append("=").append(columnName).append(";");
        }
        if (lineNumber != null) {
            builder.append(LINE_NUMBER).append("=").append(lineNumber).append(";");
        }
        return builder.toString();
    }
}
