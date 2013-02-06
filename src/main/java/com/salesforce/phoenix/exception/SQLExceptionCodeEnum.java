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
import java.util.*;
import java.util.Map.Entry;

import com.salesforce.phoenix.query.QueryConstants;


/**
 * Various SQLException code.
 * 
 * @author zhuang
 * @since 1.0
 */
public enum SQLExceptionCodeEnum {

    /** 
     * Warnings (01)
     */
    CURSOR_OPERATION_CONFLICT("01001", "Cursor operation conflict"),
    DISCONNECT_ERROR("01002", "Disconnect error"),
    DATA_TRUNCATED("01004", "Data truncated"),
    
    /**
     * Connection Exception (08)
     */
    IO_EXCEPTION("08000", "Connection closed by unknown interrupt.."),
    
    /**
     * Data Exception (22)
     */
    ILLEGAL_DATA("22000", "Illegal Data."),
    
    /**
     * Constraint Violation (23)
     */
    CONCURRENT_TABLE_MUTATION("23000", "Concurrent modification to table."),
    
    /**
     * Syntax Error or Access Rule Violation (42)
     */
    AMBIGUOUS_TABLE("42000", "Table name exists in more than one table schema and is used without being qualified."),
    AMBIGUOUS_COLUMN("42702", "Column reference ambiguous or duplicate names."),
    COLUMN_EXIST_IN_DEF("42711", "A duplicate column name was detected in the object definition or ALTER TABLE statement."),
    COLUMN_NOT_FOUND("42703", "Undefined column."),
    COLUMN_FAMILY_NOT_FOUND("42703", "Undefined column family."),
    TABLE_UNDEFINED("42P01", "Table undefined."),
    TABLE_DEPLUCATE("42P07", "Table already exists."),
    READ_ONLY_TABLE("42000", "Table is read only."),
    SCHEMA_NOT_FOUND("42Y07", "Schema not found."),
    PRIMARY_KEY_ALREADY_EXISTS("42889", "The table already has a primary key."),
    ;

    private final String sqlState;
    private final String message;

    private SQLExceptionCodeEnum(String sqlState, String message) {
        this.sqlState = sqlState;
        this.message = message;
    }

    public String getSQLState() {
        return sqlState;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "SQLException! SQLState(" + sqlState + "): " + message;
    }

    /**
     * Utility method to generate a useful exception message. It requires at least 2 string argument, 
     * the sqlState and a message. It then can take in more information about the exception. It should
     * be passed in the order of specificity. For example, schemaName, tableName, familyName, columnName. 
     */
    public static String generateSQLErrorMessage(SQLExceptionCodeEnum code) {
        return generateSQLErrorMessage(null, code, null);
    }

    public static String generateSQLErrorMessage(String message, SQLExceptionCodeEnum code) {
        return generateSQLErrorMessage(message, code, null);
    }

    public static String generateSQLErrorMessage(SQLExceptionCodeEnum code, SQLExceptionLocationInfo info) {
        return generateSQLErrorMessage(null, code, info);
    }

    public static String generateSQLErrorMessage(String message, SQLExceptionCodeEnum code, SQLExceptionLocationInfo info) {
        StringBuilder builder = new StringBuilder("SQLException. SQLState(").append(code.toString());
        if (message != null) {
            builder.append(" ").append(message);
        }
        if (info != null) {
            builder.append(" ").append(info.toString());
        }
        return builder.toString();
    }

    /**
     * Utility method to transform a SQLException into containing our formatted error messages and
     * use the SQLState code defined in this enumeration.
     */
    public static SQLException generateSQLException(Exception e, SQLExceptionCodeEnum code) {
        return new SQLException(generateSQLErrorMessage(e.getMessage(), code),
                code.getSQLState(), e);
    }

    public static SQLException generateSQLException(Exception e, SQLExceptionCodeEnum code, SQLExceptionLocationInfo info) {
        return new SQLException(generateSQLErrorMessage(e.getMessage(), code, info),
                code.getSQLState(), e);
    }

}
