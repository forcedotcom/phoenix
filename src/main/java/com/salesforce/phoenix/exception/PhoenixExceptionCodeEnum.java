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
 *
 */
public enum PhoenixExceptionCodeEnum {

    /** 
     * Warinings (01X)
     */
    CURSOR_OPERATION_CONFLICT("01001", "Cursor operation conflict"),
    DISCONNECT_ERROR("01002", "Disconnect error"),
    DATA_TRUNCATED("01004", "Data truncated"),
    
    /**
     * Syntax Error or Access Rule Violation (42X)
     */
    AMBIGUOUS_TABLE("42000", "Table name exists in more than one table schema and is used without being qualified"),
    AMBIGUOUS_COLUMN("42702", "Column reference ambiguous or duplicate names"),
    COLUMN_EXIST("42702", "A column reference is ambiguous, because of duplicate names."),
    COLUMN_EXIST_IN_TABLE_DEFINITION("42711", "A duplicate column name was detected in the object definition or ALTER TABLE statement."),
    COLUMN_NOT_FOUND("42703", "Undefined column"),
    COLUMN_FAMILY_NOT_FOUND("42703", "Undefined column family")
    ;

    private final String sqlState;
    private final String message;

    private PhoenixExceptionCodeEnum(String sqlState, String message) {
        this.sqlState = sqlState;
        this.message = message;
    }

    public String getSQLState() {
        return sqlState;
    }

    public String getMessage() {
        return message;
    }

}
