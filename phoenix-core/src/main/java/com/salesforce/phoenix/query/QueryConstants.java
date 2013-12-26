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
package com.salesforce.phoenix.query;


import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.BUFFER_LENGTH;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.CHAR_OCTET_LENGTH;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_COUNT;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_DEF;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_MODIFIER;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_SIZE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.CURRENT_VALUE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TABLE_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.DATA_TYPE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.DECIMAL_DIGITS;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.DEFAULT_COLUMN_FAMILY_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.DISABLE_WAL;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_ROWS;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.INCREMENT_BY;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.INDEX_STATE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.IS_AUTOINCREMENT;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.IS_NULLABLE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.NULLABLE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.NUM_PREC_RADIX;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.PK_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.REF_GENERATION_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.REMARKS_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SALT_BUCKETS;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SCOPE_CATALOG;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SCOPE_SCHEMA;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SCOPE_TABLE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SELF_REFERENCING_COL_NAME_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_SCHEMA;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SOURCE_DATA_TYPE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SQL_DATA_TYPE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SQL_DATETIME_SUB;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_CAT_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SEQ_NUM;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_TYPE_ID;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_NAME;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SCHEMA;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.schema.MetaDataSplitPolicy;
import com.salesforce.phoenix.schema.PName;
import com.salesforce.phoenix.schema.PNormalizedName;


/**
 * 
 * Constants used during querying
 *
 * @author jtaylor
 * @since 0.1
 */
public interface QueryConstants {
    public static final String NAME_SEPARATOR = ".";
    public final static byte[] NAME_SEPARATOR_BYTES = Bytes.toBytes(NAME_SEPARATOR);
    public static final byte NAME_SEPARATOR_BYTE = NAME_SEPARATOR_BYTES[0];
    public static final String NULL_SCHEMA_NAME = "";
    public static final String NULL_DISPLAY_TEXT = "<null>";
    public static final long UNSET_TIMESTAMP = -1;
    
    public enum JoinType {INNER, LEFT_OUTER}
    public final static String PHOENIX_SCHEMA = "system";
    public final static String PHOENIX_METADATA = "table";

    public final static PName SINGLE_COLUMN_NAME = new PNormalizedName("s");
    public final static PName SINGLE_COLUMN_FAMILY_NAME = new PNormalizedName("s");
    public final static byte[] SINGLE_COLUMN = SINGLE_COLUMN_NAME.getBytes();
    public final static byte[] SINGLE_COLUMN_FAMILY = SINGLE_COLUMN_FAMILY_NAME.getBytes();

    public static final long AGG_TIMESTAMP = HConstants.LATEST_TIMESTAMP;
    /**
     * Key used for a single row aggregation where there is no group by
     */
    public final static byte[] UNGROUPED_AGG_ROW_KEY = Bytes.toBytes("a");
    public final static PName AGG_COLUMN_NAME = SINGLE_COLUMN_NAME;
    public final static PName AGG_COLUMN_FAMILY_NAME = SINGLE_COLUMN_FAMILY_NAME;

    public static final byte[] TRUE = new byte[] {1};

    /**
     * Separator used between variable length keys for a composite key.
     * Variable length data types may not use this byte value.
     */
    public static final byte SEPARATOR_BYTE = (byte) 0;
    public static final byte[] SEPARATOR_BYTE_ARRAY = new byte[] {SEPARATOR_BYTE};
    
    public static final String DEFAULT_COPROCESS_PATH = "phoenix.jar";
    public final static int MILLIS_IN_DAY = 1000 * 60 * 60 * 24;

    public static final String EMPTY_COLUMN_NAME = "_0";
    public static final byte[] EMPTY_COLUMN_BYTES = Bytes.toBytes(EMPTY_COLUMN_NAME);
    public static final String DEFAULT_COLUMN_FAMILY = EMPTY_COLUMN_NAME;
    public static final byte[] DEFAULT_COLUMN_FAMILY_BYTES = EMPTY_COLUMN_BYTES;
    public static final String ALL_FAMILY_PROPERTIES_KEY = "";
    public static final String SYSTEM_TABLE_PK_NAME = "pk";
    
    public static final double MILLIS_TO_NANOS_CONVERTOR = Math.pow(10, 6);
    public static final BigDecimal BD_MILLIS_NANOS_CONVERSION = BigDecimal.valueOf(MILLIS_TO_NANOS_CONVERTOR);
    public static final BigDecimal BD_MILLIS_IN_DAY = BigDecimal.valueOf(QueryConstants.MILLIS_IN_DAY);
    

    public static final String CREATE_TABLE_METADATA =
            "CREATE TABLE IF NOT EXISTS " + TYPE_SCHEMA + ".\"" + TYPE_TABLE + "\"(\n" +
            // PK columns
            TENANT_ID + " VARCHAR NULL," +
            TABLE_SCHEM_NAME + " VARCHAR NULL," +
            TABLE_NAME_NAME + " VARCHAR NOT NULL," +
            COLUMN_NAME + " VARCHAR NULL," + // null only for table row
            TABLE_CAT_NAME + " VARCHAR NULL," + // using for CF - ensures uniqueness for columns
            // Table metadata (will be null for column rows)
            TABLE_TYPE_NAME + " CHAR(1)," +
            REMARKS_NAME + " VARCHAR," +
            DATA_TYPE + " INTEGER," +
            PK_NAME + " VARCHAR," +
            TYPE_NAME + " VARCHAR," +
            SELF_REFERENCING_COL_NAME_NAME + " VARCHAR," +
            REF_GENERATION_NAME + " VARCHAR," +
            TABLE_SEQ_NUM + " BIGINT," +
            COLUMN_COUNT + " INTEGER," +
            // Column metadata (will be null for table row)
            COLUMN_SIZE + " INTEGER," +
            BUFFER_LENGTH + " INTEGER," +
            DECIMAL_DIGITS + " INTEGER," +
            NUM_PREC_RADIX + " INTEGER," +
            NULLABLE + " INTEGER," +
            COLUMN_DEF + " VARCHAR," +
            SQL_DATA_TYPE + " INTEGER," +
            SQL_DATETIME_SUB + " INTEGER," +
            CHAR_OCTET_LENGTH + " INTEGER," +
            ORDINAL_POSITION + " INTEGER," +
            IS_NULLABLE + " VARCHAR," +
            SCOPE_CATALOG + " VARCHAR," +
            SCOPE_SCHEMA + " VARCHAR," +
            SCOPE_TABLE + " VARCHAR," +
            SOURCE_DATA_TYPE + " INTEGER," + // supposed to be SHORT
            IS_AUTOINCREMENT + " VARCHAR," +
            // Columns added in 1.2.1
            COLUMN_MODIFIER + " INTEGER," +
            SALT_BUCKETS + " INTEGER," +
            // Columns added in 2.0.0
            DATA_TABLE_NAME + " VARCHAR NULL," +
            INDEX_STATE + " CHAR(1),\n" +
            IMMUTABLE_ROWS + " BOOLEAN,\n" +
            // Columns added in 3.0.0
            TENANT_TYPE_ID + " VARCHAR,\n" +
            DEFAULT_COLUMN_FAMILY_NAME + " VARCHAR,\n" +
            DISABLE_WAL + " BOOLEAN\n" +
            "CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + TENANT_ID + ","
            + TABLE_SCHEM_NAME + "," + TABLE_NAME_NAME + "," + COLUMN_NAME + "," + TABLE_CAT_NAME + "))\n" +
            HConstants.VERSIONS + "=" + MetaDataProtocol.DEFAULT_MAX_META_DATA_VERSIONS + ",\n" +
            DEFAULT_COLUMN_FAMILY_NAME + "=" + "'_0'" + ",\n" + // Use original default for b/w compat
            HTableDescriptor.SPLIT_POLICY + "='" + MetaDataSplitPolicy.class.getName() + "'\n";
    
    public static final String CREATE_SEQUENCE_METADATA =
            "CREATE TABLE IF NOT EXISTS " + TYPE_SCHEMA + ".\"" + TYPE_SEQUENCE + "\"(\n" +                                    
    		SEQUENCE_SCHEMA + " VARCHAR NULL, \n" + 
            SEQUENCE_NAME +  " VARCHAR NOT NULL, \n" +
    		CURRENT_VALUE + " BIGINT NOT NULL, \n" + 
            INCREMENT_BY  + " BIGINT NOT NULL \n" + 
    		" CONSTRAINT " + SYSTEM_TABLE_PK_NAME + " PRIMARY KEY (" + SEQUENCE_SCHEMA + "," + SEQUENCE_NAME + "))\n" + 
    		HConstants.VERSIONS + "=" + 1 + "\n";
	
}
