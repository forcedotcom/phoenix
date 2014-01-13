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
import java.util.Map;

import com.google.common.collect.Maps;
import com.salesforce.hbase.index.util.IndexManagementUtil;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.AmbiguousTableException;
import com.salesforce.phoenix.schema.ColumnAlreadyExistsException;
import com.salesforce.phoenix.schema.ColumnFamilyNotFoundException;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.ConcurrentTableMutationException;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.ReadOnlyTableException;
import com.salesforce.phoenix.schema.SequenceAlreadyExistsException;
import com.salesforce.phoenix.schema.SequenceNotFoundException;
import com.salesforce.phoenix.schema.TableAlreadyExistsException;
import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.schema.TypeMismatchException;
import com.salesforce.phoenix.util.MetaDataUtil;


/**
 * Various SQLException Information. Including a vender-specific errorcode and a standard SQLState.
 * 
 * @author zhuang
 * @since 1.0
 */
public enum SQLExceptionCode {

    /**
     * Connection Exception (errorcode 01, sqlstate 08)
     */
    IO_EXCEPTION(101, "08000", "Unexpected IO exception."),
    MALFORMED_CONNECTION_URL(102, "08001", "Malformed connection url."),
    CANNOT_ESTABLISH_CONNECTION(103, "08004", "Unable to establish connection."),
    
    /**
     * Data Exception (errorcode 02, sqlstate 22)
     */
    ILLEGAL_DATA(201, "22000", "Illegal data."),
    DIVIDE_BY_ZERO(202, "22012", "Divide by zero."),
    TYPE_MISMATCH(203, "22005", "Type mismatch.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new TypeMismatchException(info.getMessage());
        }
    }),
    VALUE_IN_UPSERT_NOT_CONSTANT(204, "22008", "Values in UPSERT must evaluate to a constant."),
    MALFORMED_URL(205, "22009", "Malformed URL."),
    DATA_INCOMPATIBLE_WITH_TYPE(206, "22003", "The value is outside the range for the data type."),
    MISSING_CHAR_LENGTH(207, "22003", "Missing length for CHAR."),
    NONPOSITIVE_CHAR_LENGTH(208, "22003", "CHAR or VARCHAR must have a positive length."),
    DECIMAL_PRECISION_OUT_OF_RANGE(209, "22003", "Decimal precision outside of range. Should be within 1 and " + PDataType.MAX_PRECISION + "."),
    MISSING_BINARY_LENGTH(210, "22003", "Missing length for BINARY."),
    NONPOSITIVE_BINARY_LENGTH(211, "22003", "BINARY must have a positive length."),
    SERVER_ARITHMETIC_ERROR(212, "22012", "Arithmetic error on server."),
    VALUE_OUTSIDE_RANGE(213,"22003","Value outside range."),
    VALUE_IN_LIST_NOT_CONSTANT(214, "22008", "Values in IN must evaluate to a constant."),
    
    /**
     * Constraint Violation (errorcode 03, sqlstate 23)
     */
    CONCURRENT_TABLE_MUTATION(301, "23000", "Concurrent modification to table.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ConcurrentTableMutationException(info.getSchemaName(), info.getTableName());
        }
    }),
    CANNOT_INDEX_COLUMN_ON_TYPE(302, "23100", "The column cannot be index due to its type."),
    
    /**
     * Invalid Cursor State (errorcode 04, sqlstate 24)
     */
    CURSOR_BEFORE_FIRST_ROW(401, "24015","Cursor before first row."),
    CURSOR_PAST_LAST_ROW(402, "24016", "Cursor past last row."),
    
    /**
     * Syntax Error or Access Rule Violation (errorcode 05, sqlstate 42)
     */
    AMBIGUOUS_TABLE(501, "42000", "Table name exists in more than one table schema and is used without being qualified.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new AmbiguousTableException(info.getTableName(), info.getRootCause());
        }
    }),
    AMBIGUOUS_COLUMN(502, "42702", "Column reference ambiguous or duplicate names.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new AmbiguousColumnException(info.getColumnName(), info.getRootCause());
        }
    }),
    INDEX_MISSING_PK_COLUMNS(503, "42602", "Index table missing PK Columns."),
     COLUMN_NOT_FOUND(504, "42703", "Undefined column.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ColumnNotFoundException(info.getSchemaName(), info.getTableName(), info.getFamilyName(), info.getColumnName());
        }
    }),
    READ_ONLY_TABLE(505, "42000", "Table is read only.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ReadOnlyTableException(info.getMessage(), info.getRootCause());
        }
    }),
    CANNOT_DROP_PK(506, "42817", "Primary key column may not be dropped."),
    CANNOT_CONVERT_TYPE(507, "42846", "Cannot convert type."),
    UNSUPPORTED_ORDER_BY_QUERY(508, "42878", "ORDER BY only allowed for limited or aggregate queries"),
    PRIMARY_KEY_MISSING(509, "42888", "The table does not have a primary key."),
    PRIMARY_KEY_ALREADY_EXISTS(510, "42889", "The table already has a primary key."),
    ORDER_BY_NOT_IN_SELECT_DISTINCT(511, "42890", "All ORDER BY expressions must appear in SELECT DISTINCT:"),
    INVALID_PRIMARY_KEY_CONSTRAINT(512, "42891", "Invalid column reference in primary key constraint"),
    INVALID_ARRAY_TYPE_AS_PRIMARY_KEY_CONSTRAINT(513, "42892", "Array type not allowed as primary key constraint"),
    COLUMN_EXIST_IN_DEF(514, "42892", "A duplicate column name was detected in the object definition or ALTER TABLE statement.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ColumnAlreadyExistsException(info.getSchemaName(), info.getTableName(), info.getColumnName());
        }
    }),
    
    /** 
     * HBase and Phoenix specific implementation defined sub-classes.
     * Column family related exceptions.
     * 
     * For the following exceptions, use errorcode 10.
     */
    COLUMN_FAMILY_NOT_FOUND(1001, "42I01", "Undefined column family.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new ColumnFamilyNotFoundException(info.getFamilyName());
        }
    }),
    PROPERTIES_FOR_FAMILY(1002, "42I02","Properties may not be defined for an unused family name."),
    // Primary/row key related exceptions.
    PRIMARY_KEY_WITH_FAMILY_NAME(1003, "42J01", "Primary key columns must not have a family name."),
    PRIMARY_KEY_OUT_OF_ORDER(1004, "42J02", "Order of columns in primary key constraint must match the order in which they're declared."),
    VARBINARY_IN_ROW_KEY(1005, "42J03", "The VARBINARY type can only be used as the last part of a multi-part row key."),
    NOT_NULLABLE_COLUMN_IN_ROW_KEY(1006, "42J04", "Only nullable columns may be added to a multi-part row key."),
    VARBINARY_LAST_PK(1015, "42J04", "Cannot add column to table when the last PK column is of type VARBINARY."),
    NULLABLE_FIXED_WIDTH_LAST_PK(1023, "42J04", "Cannot add column to table when the last PK column is nullable and fixed width."),
    CANNOT_MODIFY_PK_OF_DERIVED_TABLE(1036, "42J04", "Cannot modify PK of a derived table."),
    BASE_TABLE_COLUMN(1037, "42J04", "Cannot modify columns of base table used by tenant-specific tables."),
    // Key/value column related errors
    KEY_VALUE_NOT_NULL(1007, "42K01", "A key/value column may not be declared as not null."),
    // View related errors.
    VIEW_WITH_TABLE_CONFIG(1008, "42L01", "A view may not contain table configuration properties."),
    VIEW_WITH_PROPERTIES(1009, "42L02", "Properties may not be defined for a view."),
    // Table related errors that are not in standard code.
    CANNOT_MUTATE_TABLE(1010, "42M01", "Not allowed to mutate table."),
    UNEXPECTED_MUTATION_CODE(1011, "42M02", "Unexpected mutation code."),
    TABLE_UNDEFINED(1012, "42M03", "Table undefined.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new TableNotFoundException(info.getSchemaName(), info.getTableName());
        }
    }),
    TABLE_ALREADY_EXIST(1013, "42M04", "Table already exists.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new TableAlreadyExistsException(info.getSchemaName(), info.getTableName());
        }
    }),

    // Syntax error
    TYPE_NOT_SUPPORTED_FOR_OPERATOR(1014, "42Y01", "The operator does not support the operand type."),
    AGGREGATE_IN_GROUP_BY(1016, "42Y26", "Aggregate expressions may not be used in GROUP BY."),
    AGGREGATE_IN_WHERE(1017, "42Y26", "Aggregate may not be used in WHERE."),
    AGGREGATE_WITH_NOT_GROUP_BY_COLUMN(1018, "42Y27", "Aggregate may not contain columns not in GROUP BY."),
    ONLY_AGGREGATE_IN_HAVING_CLAUSE(1019, "42Y26", "Only aggregate maybe used in the HAVING clause."),
    UPSERT_COLUMN_NUMBERS_MISMATCH(1020, "42Y60", "Number of columns upserting must match number of values."),
    // Table properties exception.
    INVALID_BUCKET_NUM(1021, "42Y80", "Salt bucket numbers should be with 1 and 256."),
    NO_SPLITS_ON_SALTED_TABLE(1022, "42Y81", "Should not specify split points on salted table with default row key order."),
    SALT_ONLY_ON_CREATE_TABLE(1024, "42Y83", "Salt bucket number may only be specified when creating a table."),
    SET_UNSUPPORTED_PROP_ON_ALTER_TABLE(1025, "42Y84", "Unsupported property set in ALTER TABLE command."),
    CANNOT_ADD_NOT_NULLABLE_COLUMN(1038, "42Y84", "Only nullable columns may be added for a pre-existing table."),
    NO_MUTABLE_INDEXES(1026, "42Y85", "Mutable secondary indexes are only supported for HBase version " + MetaDataUtil.decodeHBaseVersionAsString(PhoenixDatabaseMetaData.MUTABLE_SI_VERSION_THRESHOLD) + " and above."),
    NO_DELETE_IF_IMMUTABLE_INDEX(1027, "42Y86", "Delete not allowed on a table with IMMUTABLE_ROW with non PK column in index."),
    INVALID_INDEX_STATE_TRANSITION(1028, "42Y87", "Invalid index state transition."),
    INVALID_MUTABLE_INDEX_CONFIG(1029, "42Y88", "Mutable secondary indexes must have the " 
            + IndexManagementUtil.WAL_EDIT_CODEC_CLASS_KEY + " property set to " 
            +  IndexManagementUtil.INDEX_WAL_EDIT_CODEC_CLASS_NAME + " in the hbase-sites.xml of every region server"),
            
            
    CANNOT_CREATE_TENANT_SPECIFIC_BASE_TABLE(1030, "42Y89", "Cannot create base table for tenant-specific connection"),
    CREATE_TENANT_TABLE_NO_PK(1031, "42Y90", "Defining PK columns not allowed for tenant-specific tables."),
    BASE_TABLE_NOT_TOP_LEVEL(1032, "42Y91", "Base table for a tenant table-specific table must be top level."),
    DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE(1034, "42Y93", "Default column family may only be specified when creating a table."),
    TYPE_ID_USED(1039, "42Y95", "Type id is already used by this tenant for this base table."),
    INSUFFICIENT_MULTI_TENANT_COLUMNS(1040, "42Y96", "A MULTI_TENANT table must have 2 or more PK columns with the first column being VARCHAR or CHAR."),
    INSUFFICIENT_MULTI_TENANT_TYPE_COLUMNS(1041, "42Y97", "A MULTI_TENANT, MULTI_TYPE table must have 3 or more PK columns with the first column being VARCHAR or CHAR."),
    CANNOT_CREATE_TYPELESS_BASE_TABLE(1042, "42Y98", "Cannot derive top level table without specifying AS type ID clause"),
    NO_TENANT_ID_FOR_MULTI_TENANT(1043, "42Y99", "Must use tenant-specific connection to derive from multi-tenant table"),
    NO_TYPE_ID_FOR_MULTI_TYPE(1044, "43A01", "Must specify AS type ID clause to derive from multi-type table"),
    TYPE_ID_NOT_CONSTANT(1045, "43A02", "AS type ID clause must evaluate to a constant."),
    INSUFFICIENT_MULTI_TYPE_COLUMNS(1046, "43A03", "A MULTI_TYPE table must have 2 or more PK columns."),
        
    /** Sequence related */
    SEQUENCE_ALREADY_EXIST(1200, "42Z00", "Sequence already exists.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SequenceAlreadyExistsException(info.getSchemaName(), info.getTableName());
        }
    }),
    SEQUENCE_UNDEFINED(1201, "42Z01", "Sequence undefined.", new Factory() {
        @Override
        public SQLException newException(SQLExceptionInfo info) {
            return new SequenceNotFoundException(info.getSchemaName(), info.getTableName());
        }
    }),
    STARTS_WITH_MUST_BE_CONSTANT(1202, "42Z02", "Sequence STARTS WITH value must be an integer or long constant."),
    INCREMENT_BY_MUST_BE_CONSTANT(1203, "42Z03", "Sequence INCREMENT BY value must be an integer or long constant."),
    CACHE_MUST_BE_NON_NEGATIVE_CONSTANT(1204, "42Z04", "Sequence CACHE value must be a non negative integer constant."),
    INVALID_USE_OF_NEXT_VALUE_FOR(1205, "42Z05", "NEXT VALUE FOR may only be used as in a SELECT or an UPSERT VALUES expression."),
    CANNOT_CALL_CURRENT_BEFORE_NEXT_VALUE(1206, "42Z06", "NEXT VALUE FOR must be called before CURRENT VALUE FOR is called."),
    EMPTY_SEQUENCE_CACHE(1207, "42Z07", "No more cached sequence values"),

    /** Parser error. (errorcode 06, sqlState 42P) */
    PARSER_ERROR(601, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    MISSING_TOKEN(602, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    UNWANTED_TOKEN(603, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    MISMATCHED_TOKEN(604, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    UNKNOWN_FUNCTION(605, "42P00", "Syntax error.", Factory.SYTAX_ERROR),
    
    /**
     * Implementation defined class. Execution exceptions (errorcode 11, sqlstate XCL). 
     */
    RESULTSET_CLOSED(1101, "XCL01", "ResultSet is closed."),
    GET_TABLE_REGIONS_FAIL(1102, "XCL02", "Cannot get all table regions"),
    EXECUTE_QUERY_NOT_APPLICABLE(1103, "XCL03", "executeQuery may not be used."),
    EXECUTE_UPDATE_NOT_APPLICABLE(1104, "XCL03", "executeUpdate may not be used."),
    SPLIT_POINT_NOT_CONSTANT(1105, "XCL04", "Split points must be constants."),
    
    /**
     * Implementation defined class. Phoenix internal error. (errorcode 20, sqlstate INT).
     */
    CANNOT_CALL_METHOD_ON_TYPE(2001, "INT01", "Cannot call method on the argument type."),
    CLASS_NOT_UNWRAPPABLE(2002, "INT03", "Class not unwrappable"),
    PARAM_INDEX_OUT_OF_BOUND(2003, "INT04", "Parameter position is out of range."),
    PARAM_VALUE_UNBOUND(2004, "INT05", "Parameter value unbound"),
    INTERRUPTED_EXCEPTION(2005, "INT07", "Interrupted exception."),
    INCOMPATIBLE_CLIENT_SERVER_JAR(2006, "INT08", "Incompatible jars detected between client and server."),
    OUTDATED_JARS(2007, "INT09", "Outdated jars."),
    INDEX_METADATA_NOT_FOUND(2008, "INT10", "Unable to find cached index metadata. "),
    UNKNOWN_ERROR_CODE(2009, "INT11", "Unknown error code"),
    ;

    private final int errorCode;
    private final String sqlState;
    private final String message;
    private final Factory factory;

    private SQLExceptionCode(int errorCode, String sqlState, String message) {
        this(errorCode, sqlState, message, Factory.DEFAULTY);
    }

    private SQLExceptionCode(int errorCode, String sqlState, String message, Factory factory) {
        this.errorCode = errorCode;
        this.sqlState = sqlState;
        this.message = message;
        this.factory = factory;
    }

    public String getSQLState() {
        return sqlState;
    }

    public String getMessage() {
        return message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return "ERROR " + errorCode + " (" + sqlState + "): " + message;
    }

    public Factory getExceptionFactory() {
        return factory;
    }

    public static interface Factory {
        public static final Factory DEFAULTY = new Factory() {

            @Override
            public SQLException newException(SQLExceptionInfo info) {
                return new SQLException(info.toString(), info.getCode().getSQLState(), info.getCode().getErrorCode(), info.getRootCause());
            }
            
        };
        public static final Factory SYTAX_ERROR = new Factory() {

            @Override
            public SQLException newException(SQLExceptionInfo info) {
                return new PhoenixParserException(info.getMessage(), info.getRootCause());
            }
            
        };
        public SQLException newException(SQLExceptionInfo info);
    }
    
    private static final Map<Integer,SQLExceptionCode> errorCodeMap = Maps.newHashMapWithExpectedSize(SQLExceptionCode.values().length);
    static {
        for (SQLExceptionCode code : SQLExceptionCode.values()) {
            SQLExceptionCode otherCode = errorCodeMap.put(code.getErrorCode(), code);
            if (otherCode != null) {
                throw new IllegalStateException("Duplicate error code for " + code + " and " + otherCode);
            }
        }
    }
    
    public static SQLExceptionCode fromErrorCode(int errorCode) throws SQLException {
        SQLExceptionCode code = errorCodeMap.get(errorCode);
        if (code == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.UNKNOWN_ERROR_CODE)
            .setMessage(Integer.toString(errorCode)).build().buildException();
        }
        return code;
    }
}
