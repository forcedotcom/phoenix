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
 * Various SQLException code.
 * 
 * @author zhuang
 * @since 1.0
 */
public enum SQLExceptionCodeEnum {

    /** 
     * Warnings (01)
     */
    CURSOR_OPERATION_CONFLICT("01001", "Cursor operation conflict."),
    DISCONNECT_ERROR("01002", "Disconnect error."),
    DATA_TRUNCATED("01004", "Data truncated."),
    
    /**
     * Connection Exception (08)
     */
    IO_EXCEPTION("08000", "Connection closed by unknown interrupt."),
    
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
    PRIMARY_KEY_MISSING("42888", "The table does not have a primary key."),
    PRIMARY_KEY_ALREADY_EXISTS("42889", "The table already has a primary key."),
    PRIMARY_KEY_WITH_FAMILY_NAME("42000", "Primary key should not have a family name."),
    KEY_VALUE_NOT_NULL("42000", "A key/value column may not be declared as NOT NULL."),
    VIEW_WITH_TABLE_CONFIG("42000", "A VIEW may not contain table configuration properties."),
    VIEW_WITH_PROPERTIES("42000", "Properties may not be defined for a VIEW."),
    PRIMARY_KEY_OUT_OF_ORDER("42000", "Order of columns in PRIMARY KEY constraint must match the order in which they're declared."),
    NO_BINARY_IN_ROW_KEY("42000", "The BINARY type may not be used as part of a multi-part row key."),
    PROPERTIES_FOR_FAMILY("42000","Properties may not be defined for an unused family name."),
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

}
