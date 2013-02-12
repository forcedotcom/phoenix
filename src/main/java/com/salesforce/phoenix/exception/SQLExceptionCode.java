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
public enum SQLExceptionCode {

    /** 
     * Warnings (01)
     */
    DISCONNECT_ERROR("01002", "Disconnect error."),
    MALFORMED_ZOOKEEPER_URL("01501", "Malformed Zookeeper connection url."),
    
    /**
     * Connection Exception (08)
     */
    IO_EXCEPTION("08000", "Unexpected IO exception."),
    CANNOT_ESTABLISH_CONNECTION("08004", "Unable to establish connection."),
    
    /**
     * Data Exception (22)
     */
    ILLEGAL_DATA("22000", "Illegal data."),
    DIVIDE_BY_ZERO("22012", "Divide by zero."),
    TYPE_MISMATCH("22005", "Type mismatch."),
    VALUE_IN_UPSERT_NOT_CONSTANT("22008", "Values in UPSERT must evaluate to a constant"),
    
    /**
     * Constraint Violation (23)
     */
    CONCURRENT_TABLE_MUTATION("23000", "Concurrent modification to table."),
    
    /**
     * Invalid Cursor State (24)
     */
    CURSOR_BEFORE_FIRST_ROW("24015","Cursor before first tow."),
    CURSOR_PAST_LAST_ROW("24016", "Cursor past last row."),
    
    /**
     * Syntax Error or Access Rule Violation (42)
     */
    AMBIGUOUS_TABLE("42000", "Table name exists in more than one table schema and is used without being qualified."),
    AMBIGUOUS_COLUMN("42702", "Column reference ambiguous or duplicate names."),
    COLUMN_EXIST_IN_DEF("42711", "A duplicate column name was detected in the object definition or ALTER TABLE statement."),
    COLUMN_NOT_FOUND("42703", "Undefined column."),
    READ_ONLY_TABLE("42000", "Table is read only."),
    CANNOT_DROP_PK("42817", "Primary key column may not be dropped."),
    CANNOT_CONVERT_TYPE("42846", "Cannot convert type."),
    UNSUPPORTED_ORDER_BY_QUERY("42878", "ORDER BY only allowed for limited or aggregate queries"),
    PRIMARY_KEY_MISSING("42888", "The table does not have a primary key."),
    PRIMARY_KEY_ALREADY_EXISTS("42889", "The table already has a primary key."),
    // HBase and Phoenix specific implementation defined sub-classes.
    // Column family related exceptions.
    COLUMN_FAMILY_NOT_FOUND("42I01", "Undefined column family."),
    PROPERTIES_FOR_FAMILY("42I02","Properties may not be defined for an unused family name."),
    // Primary/row key related exceptions.
    PRIMARY_KEY_WITH_FAMILY_NAME("42J01", "Primary key should not have a family name."),
    PRIMARY_KEY_OUT_OF_ORDER("42J02", "Order of columns in primary key constraint must match the order in which they're declared."),
    BINARY_IN_ROW_KEY("42J03", "The BINARY type may not be used as part of a multi-part row key."),
    NOT_NULLABLE_COLUMN_IN_ROW_KEY("42J04", "Only nullable columns may be added to a multi-part row key."),
    // Key/value column related errors
    KEY_VALUE_NOT_NULL("42K01", "A key/value column may not be declared as not null."),
    // View related errors.
    VIEW_WITH_TABLE_CONFIG("42L01", "A VIEW may not contain table configuration properties."),
    VIEW_WITH_PROPERTIES("42L02", "Properties may not be defined for a view."),
    // Table related errors that are not in standard code.
    CANNOT_MUTATE_TABLE("42M01", "Not allowed to mutate table."),
    UNEXPECTED_MUTATION_CODE("42M02", "Unexpected mutation code."),
    TABLE_UNDEFINED("42M03", "Table undefined."),
    TABLE_ALREADY_EXIST("42M04", "Table already exists."),
    // Parser error
    PARSER_ERROR("42P00", "Parser error."),
    // Syntax error
    TYPE_NOT_SUPPORTED_FOR_OPERATOR("42Y01", "The operator does not support the operand type."),
    SCHEMA_NOT_FOUND("42Y07", "Schema not found."),
    AGGREGATE_IN_GROUP_BY("42Y26", "Aggregate expressions may not be used in GROUP BY."),
    AGGREGATE_IN_WHERE("42Y26", "Aggregate may not be used in WHERE."),
    AGGREGATE_WITH_NOT_GROUP_BY_COLUMN("42Y27", "Aggregate may not contain columns not in GROUP BY."),
    ONLY_AGGREGATE_IN_HAVING_CLAUSE("42Y26", "Only aggregate maybe used in the HAVING clause."),
    UPSERT_COLUMN_NUMBERS_MISMATCH("42Y60", "Number of columns upserting must match number of values."),
    
    /**
     * Implementation defined class. Execution exceptions (XCL). 
     */
    RESULTSET_CLOSED("XCL01", "ResultSet is closed."),
    GET_TABLE_REGIONS_FAIL("XCL02", "Cannot get all table regions"),
    EXECUTE_QUERY_NOT_APPLICABLE("XCL03", "executeQuery may not be used."),
    EXECUTE_UPDATE_NOT_APPLICABLE("XCL03", "executeUpdate may not be used."),
    SPLIT_POINT_NOT_CONSTANT("XCL04", "Split points must be constants."),
    
    /**
     * Implementation defined class. Phoenix internal error. (INT).
     */
    INTERNAL_ERROR("INT00", "Internal error."), // General error, should have a message for details.
    CANNOT_CALL_METHOD_ON_TYPE("INT01", "Cannot call method on the argument type."),
    MALFORMED_URL("INT02", "Malformed URL."),
    CLASS_NOT_UNWRAPPABLE("INT03", "Class not unwrappable"),
    PARAM_INDEX_OUT_OF_BOUND("INT04", "Parameter position is out of range."),
    PARAM_VALUE_UNBOUND("INT05", "Parameter value unbound"),
    INTERRUPTED_EXCEPTION("INT07", "Interrupted exception."),
    INCOMPATIBLE_CLIENT_SERVER_JAR("INT08", "Incompatible jars detected between client and server."),
    OUTDATED_JARS("INT09", "Outdated jars."),
    ;

    private final String sqlState;
    private final String message;

    private SQLExceptionCode(String sqlState, String message) {
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
        return "SQLState(" + sqlState + "): " + message;
    }

}
