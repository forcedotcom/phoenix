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

import static com.salesforce.phoenix.util.TestUtil.ATABLE_NAME;
import static com.salesforce.phoenix.util.TestUtil.BTABLE_NAME;
import static com.salesforce.phoenix.util.TestUtil.CUSTOM_ENTITY_DATA_FULL_NAME;
import static com.salesforce.phoenix.util.TestUtil.ENTITY_HISTORY_SALTED_TABLE_NAME;
import static com.salesforce.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static com.salesforce.phoenix.util.TestUtil.FUNKY_NAME;
import static com.salesforce.phoenix.util.TestUtil.GROUPBYTEST_NAME;
import static com.salesforce.phoenix.util.TestUtil.HBASE_DYNAMIC_COLUMNS;
import static com.salesforce.phoenix.util.TestUtil.HBASE_NATIVE;
import static com.salesforce.phoenix.util.TestUtil.INDEX_DATA_SCHEMA;
import static com.salesforce.phoenix.util.TestUtil.INDEX_DATA_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_CUSTOMER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_ITEM_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_ORDER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.JOIN_SUPPLIER_TABLE;
import static com.salesforce.phoenix.util.TestUtil.KEYONLY_NAME;
import static com.salesforce.phoenix.util.TestUtil.MDTEST_NAME;
import static com.salesforce.phoenix.util.TestUtil.MULTI_CF_NAME;
import static com.salesforce.phoenix.util.TestUtil.MUTABLE_INDEX_DATA_TABLE;
import static com.salesforce.phoenix.util.TestUtil.PRODUCT_METRICS_NAME;
import static com.salesforce.phoenix.util.TestUtil.PTSDB2_NAME;
import static com.salesforce.phoenix.util.TestUtil.PTSDB3_NAME;
import static com.salesforce.phoenix.util.TestUtil.PTSDB_NAME;
import static com.salesforce.phoenix.util.TestUtil.STABLE_NAME;
import static com.salesforce.phoenix.util.TestUtil.TABLE_WITH_SALTING;
import static com.salesforce.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.junit.AfterClass;

import com.google.common.collect.ImmutableMap;
import com.salesforce.phoenix.jdbc.PhoenixTestDriver;
import com.salesforce.phoenix.schema.TableAlreadyExistsException;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.ReadOnlyProps;

public abstract class BaseTest {
    private static final Map<String,String> tableDDLMap;
    static {
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();
        builder.put(ENTITY_HISTORY_TABLE_NAME,"create table " + ENTITY_HISTORY_TABLE_NAME +
                "   (organization_id char(15) not null,\n" +
                "    parent_id char(15) not null,\n" +
                "    created_date date not null,\n" +
                "    entity_history_id char(15) not null,\n" +
                "    old_value varchar,\n" +
                "    new_value varchar\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, parent_id, created_date, entity_history_id)\n" +
                ")");
        builder.put(ENTITY_HISTORY_SALTED_TABLE_NAME,"create table " + ENTITY_HISTORY_SALTED_TABLE_NAME +
                "   (organization_id char(15) not null,\n" +
                "    parent_id char(15) not null,\n" +
                "    created_date date not null,\n" +
                "    entity_history_id char(15) not null,\n" +
                "    old_value varchar,\n" +
                "    new_value varchar\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, parent_id, created_date, entity_history_id))\n" +
                "    SALT_BUCKETS = 4");
        builder.put(ATABLE_NAME,"create table " + ATABLE_NAME +
                "   (organization_id char(15) not null, \n" +
                "    entity_id char(15) not null,\n" +
                "    a_string varchar(100),\n" +
                "    b_string varchar(100),\n" +
                "    a_integer integer,\n" +
                "    a_date date,\n" +
                "    a_time time,\n" +
                "    a_timestamp timestamp,\n" +
                "    x_decimal decimal(31,10),\n" +
                "    x_long bigint,\n" +
                "    x_integer integer,\n" +
                "    y_integer integer,\n" +
                "    a_byte tinyint,\n" +
                "    a_short smallint,\n" +
                "    a_float float,\n" +
                "    a_double double,\n" +
                "    a_unsigned_float unsigned_float,\n" +
                "    a_unsigned_double unsigned_double\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n" +
                ")");
        builder.put(BTABLE_NAME,"create table " + BTABLE_NAME +
                "   (a_string varchar not null, \n" +
                "    a_id char(3) not null,\n" +
                "    b_string varchar not null, \n" +
                "    a_integer integer not null, \n" +
                "    c_string varchar(2) null,\n" +
                "    b_integer integer,\n" +
                "    c_integer integer,\n" +
                "    d_string varchar(3),\n" +
                "    e_string char(10)\n" +
                "    CONSTRAINT my_pk PRIMARY KEY (a_string,a_id,b_string,a_integer,c_string))");
        builder.put(TABLE_WITH_SALTING,"create table " + TABLE_WITH_SALTING +
                "   (a_integer integer not null, \n" +
                "    a_string varchar not null, \n" +
                "    a_id char(3) not null,\n" +
                "    b_string varchar, \n" +
                "    b_integer integer \n" +
                "    CONSTRAINT pk PRIMARY KEY (a_integer, a_string, a_id))\n" +
                "    SALT_BUCKETS = 4");
        builder.put(STABLE_NAME,"create table " + STABLE_NAME +
                "   (id char(1) not null primary key,\n" +
                "    value integer)");
        builder.put(PTSDB_NAME,"create table " + PTSDB_NAME +
                "   (inst varchar null,\n" +
                "    host varchar null,\n" +
                "    date date not null,\n" +
                "    val decimal(31,10)\n" +
                "    CONSTRAINT pk PRIMARY KEY (inst, host, date))");
        builder.put(PTSDB2_NAME,"create table " + PTSDB2_NAME +
                "   (inst varchar(10) not null,\n" +
                "    date date not null,\n" +
                "    val1 decimal,\n" +
                "    val2 decimal(31,10),\n" +
                "    val3 decimal\n" +
                "    CONSTRAINT pk PRIMARY KEY (inst, date))");
        builder.put(PTSDB3_NAME,"create table " + PTSDB3_NAME +
                "   (host varchar(10) not null,\n" +
                "    date date not null,\n" +
                "    val1 decimal,\n" +
                "    val2 decimal(31,10),\n" +
                "    val3 decimal\n" +
                "    CONSTRAINT pk PRIMARY KEY (host DESC, date DESC))");
        builder.put(FUNKY_NAME,"create table " + FUNKY_NAME +
                "   (\"foo!\" varchar not null primary key,\n" +
                "    \"1\".\"#@$\" varchar, \n" +
                "    \"1\".\"foo.bar-bas\" varchar, \n" +
                "    \"1\".\"Value\" integer,\n" +
                "    \"1\".\"VALUE\" integer,\n" +
                "    \"1\".\"value\" integer,\n" +
                "    \"1\".\"_blah^\" varchar)"
                );
        builder.put(KEYONLY_NAME,"create table " + KEYONLY_NAME +
                "   (i1 integer not null, i2 integer not null\n" +
                "    CONSTRAINT pk PRIMARY KEY (i1,i2))");
        builder.put(MDTEST_NAME,"create table " + MDTEST_NAME +
                "   (id char(1) not null primary key,\n" +
                "    a.col1 integer,\n" +
                "    b.col2 bigint,\n" +
                "    b.col3 decimal,\n" +
                "    b.col4 decimal(5),\n" +
                "    b.col5 decimal(6,3))\n" +
                "    a." + HConstants.VERSIONS + "=" + 1 + "," + "a." + HColumnDescriptor.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE +  "'");
        builder.put(MULTI_CF_NAME,"create table " + MULTI_CF_NAME +
                "   (id char(15) not null primary key,\n" +
                "    a.unique_user_count integer,\n" +
                "    b.unique_org_count integer,\n" +
                "    c.db_cpu_utilization decimal(31,10),\n" +
                "    d.transaction_count bigint,\n" +
                "    e.cpu_utilization decimal(31,10),\n" +
                "    f.response_time bigint,\n" +
                "    g.response_time bigint)");
        builder.put(GROUPBYTEST_NAME,"create table " + GROUPBYTEST_NAME +
                "   (id varchar not null primary key,\n" +
                "    uri varchar, appcpu integer)");
        builder.put(HBASE_NATIVE,"create table " + HBASE_NATIVE +
                "   (uint_key unsigned_int not null," +
                "    ulong_key unsigned_long not null," +
                "    string_key varchar not null,\n" +
                "    \"1\".uint_col unsigned_int," +
                "    \"1\".ulong_col unsigned_long" +
                "    CONSTRAINT pk PRIMARY KEY (uint_key, ulong_key, string_key))\n" +
                     HColumnDescriptor.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE + "'");
        builder.put(HBASE_DYNAMIC_COLUMNS,"create table " + HBASE_DYNAMIC_COLUMNS + 
                "   (entry varchar not null," +
                "    F varchar," +
                "    A.F1v1 varchar," +
                "    A.F1v2 varchar," +
                "    B.F2v1 varchar" +
                "    CONSTRAINT pk PRIMARY KEY (entry))\n");
        builder.put(PRODUCT_METRICS_NAME,"create table " + PRODUCT_METRICS_NAME +
                "   (organization_id char(15) not null," +
                "    date date not null," +
                "    feature char(1) not null," +
                "    unique_users integer not null,\n" +
                "    db_utilization decimal(31,10),\n" +
                "    transactions bigint,\n" +
                "    cpu_utilization decimal(31,10),\n" +
                "    response_time bigint,\n" +
                "    io_time bigint,\n" +
                "    region varchar,\n" +
                "    unset_column decimal(31,10)\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, DATe, feature, UNIQUE_USERS))");
        builder.put(CUSTOM_ENTITY_DATA_FULL_NAME,"create table " + CUSTOM_ENTITY_DATA_FULL_NAME +
                "   (organization_id char(15) not null, \n" +
                "    key_prefix char(3) not null,\n" +
                "    custom_entity_data_id char(12) not null,\n" +
                "    created_by varchar,\n" +
                "    created_date date,\n" +
                "    currency_iso_code char(3),\n" +
                "    deleted char(1),\n" +
                "    division decimal(31,10),\n" +
                "    last_activity date,\n" +
                "    last_update date,\n" +
                "    last_update_by varchar,\n" +
                "    name varchar(240),\n" +
                "    owner varchar,\n" +
                "    record_type_id char(15),\n" +
                "    setup_owner varchar,\n" +
                "    system_modstamp date,\n" +
                "    b.val0 varchar,\n" +
                "    b.val1 varchar,\n" +
                "    b.val2 varchar,\n" +
                "    b.val3 varchar,\n" +
                "    b.val4 varchar,\n" +
                "    b.val5 varchar,\n" +
                "    b.val6 varchar,\n" +
                "    b.val7 varchar,\n" +
                "    b.val8 varchar,\n" +
                "    b.val9 varchar\n" +
                "    CONSTRAINT pk PRIMARY KEY (organization_id, key_prefix, custom_entity_data_id))");
        builder.put("IntKeyTest","create table IntKeyTest" +
                "   (i integer not null primary key)");
        builder.put("IntIntKeyTest","create table IntIntKeyTest" +
                "   (i integer not null primary key, j integer)");
        builder.put("LongInKeyTest","create table LongInKeyTest" +
                "   (l bigint not null primary key)");
        builder.put("PKIntValueTest", "create table PKIntValueTest" +
                "   (pk integer not null primary key)");
        builder.put("PKBigIntValueTest", "create table PKBigIntValueTest" +
                "   (pk bigint not null primary key)");
        builder.put("PKUnsignedIntValueTest", "create table PKUnsignedIntValueTest" +
                "   (pk unsigned_int not null primary key)");
        builder.put("PKUnsignedLongValueTest", "create table PKUnsignedLongValueTest" +
                "   (pk unsigned_long not null\n" +
                "    CONSTRAINT pk PRIMARY KEY (pk))");
        builder.put("KVIntValueTest", "create table KVIntValueTest" +
                "   (pk integer not null primary key,\n" +
                "    kv integer)\n");
        builder.put("KVBigIntValueTest", "create table KVBigIntValueTest" +
                "   (pk integer not null primary key,\n" +
                "    kv bigint)\n");
        builder.put(INDEX_DATA_TABLE, "create table " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + INDEX_DATA_TABLE + "(" +
                "   varchar_pk VARCHAR NOT NULL, " +
                "   char_pk CHAR(5) NOT NULL, " +
                "   int_pk INTEGER NOT NULL, "+ 
                "   long_pk BIGINT NOT NULL, " +
                "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
                "   a.varchar_col1 VARCHAR, " +
                "   a.char_col1 CHAR(5), " +
                "   a.int_col1 INTEGER, " +
                "   a.long_col1 BIGINT, " +
                "   a.decimal_col1 DECIMAL(31, 10), " +
                "   b.varchar_col2 VARCHAR, " +
                "   b.char_col2 CHAR(5), " +
                "   b.int_col2 INTEGER, " +
                "   b.long_col2 BIGINT, " +
                "   b.decimal_col2 DECIMAL(31, 10) " +
                "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk)) " +
                "IMMUTABLE_ROWS=true");
        builder.put(MUTABLE_INDEX_DATA_TABLE, "create table " + INDEX_DATA_SCHEMA + QueryConstants.NAME_SEPARATOR + MUTABLE_INDEX_DATA_TABLE + "(" +
                "   varchar_pk VARCHAR NOT NULL, " +
                "   char_pk CHAR(5) NOT NULL, " +
                "   int_pk INTEGER NOT NULL, "+ 
                "   long_pk BIGINT NOT NULL, " +
                "   decimal_pk DECIMAL(31, 10) NOT NULL, " +
                "   a.varchar_col1 VARCHAR, " +
                "   a.char_col1 CHAR(5), " +
                "   a.int_col1 INTEGER, " +
                "   a.long_col1 BIGINT, " +
                "   a.decimal_col1 DECIMAL(31, 10), " +
                "   b.varchar_col2 VARCHAR, " +
                "   b.char_col2 CHAR(5), " +
                "   b.int_col2 INTEGER, " +
                "   b.long_col2 BIGINT, " +
                "   b.decimal_col2 DECIMAL(31, 10) " +
                "   CONSTRAINT pk PRIMARY KEY (varchar_pk, char_pk, int_pk, long_pk DESC, decimal_pk)) "
                );
        builder.put("SumDoubleTest","create table SumDoubleTest" +
                "   (id varchar not null primary key, d DOUBLE, f FLOAT, ud UNSIGNED_DOUBLE, uf UNSIGNED_FLOAT, i integer, de decimal)");
        builder.put(JOIN_ORDER_TABLE, "create table " + JOIN_ORDER_TABLE +
                "   (order_id char(15) not null primary key, " +
                "    customer_id char(10) not null, " +
                "    item_id char(10) not null, " +
                "    quantity integer not null, " +
                "    date date not null)");
        builder.put(JOIN_CUSTOMER_TABLE, "create table " + JOIN_CUSTOMER_TABLE +
                "   (customer_id char(10) not null primary key, " +
                "    name varchar not null, " +
                "    phone char(12), " +
                "    address varchar, " +
                "    loc_id char(5))");
        builder.put(JOIN_ITEM_TABLE, "create table " + JOIN_ITEM_TABLE +
                "   (item_id char(10) not null primary key, " +
                "    name varchar not null, " +
                "    price integer not null, " +
                "    supplier_id char(10) not null, " +
                "    description varchar)");
        builder.put(JOIN_SUPPLIER_TABLE, "create table " + JOIN_SUPPLIER_TABLE +
                "   (supplier_id char(10) not null primary key, " +
                "    name varchar not null, " +
                "    phone char(12), " +
                "    address varchar, " +
    			"    loc_id char(5))");
        tableDDLMap = builder.build();
    }

    private static final String ORG_ID = "00D300000000XHP";

    protected static String getOrganizationId() {
        return ORG_ID;
    }

    private static long timestamp;

    public static long nextTimestamp() {
        timestamp += 100;
        return timestamp;
    }

    protected static PhoenixTestDriver driver;
    private static int driverRefCount = 0;

    protected static synchronized PhoenixTestDriver initDriver(QueryServices services) throws Exception {
        if (driver == null) {
            if (driverRefCount == 0) {
                BaseTest.driver = new PhoenixTestDriver(services);
                DriverManager.registerDriver(driver);
                driverRefCount++;
            }
        }
        return BaseTest.driver;
    }

    // We need to deregister an already existing driver in order
    // to register a new one. We need to create a new one so that
    // we register the new one with the new Configuration instance.
    // Otherwise, we get connection errors because the prior one
    // is no longer associated with the miniCluster.
    protected static synchronized void destroyDriver() {
        if (driver != null) {
            driverRefCount--;
            if (driverRefCount == 0) {
                try {
                    try {
                        driver.close();
                    } finally {
                        try {
                            DriverManager.deregisterDriver(driver);
                        } finally {
                            driver = null;
                        }
                    }
                } catch (SQLException e) {
                }
            }
        }
    }

    protected static void startServer(String url, ReadOnlyProps props) throws Exception {
//        Pass config through initDriver for testing purposes
//        Don't create Config or surface getConfig or getProps in QueryServices
//        Create Config in ConnectionQueryServices, but don't surface it
//        Add getAdmin in ConnectionQueryServices
        PhoenixTestDriver driver = initDriver(new QueryServicesTestImpl(props));
        assertTrue(DriverManager.getDriver(url) == driver);
        driver.connect(url, TEST_PROPERTIES);
    }
    
    protected static void startServer(String url) throws Exception {
        startServer(url, ReadOnlyProps.EMPTY_PROPS);
    }

    protected static void stopServer() throws Exception {
        destroyDriver();
    }

    @AfterClass
    public static void doTeardown() throws Exception {
        stopServer();
    }

    protected static void ensureTableCreated(String url, String tableName) throws SQLException {
        ensureTableCreated(url, tableName, null, null);
    }

    protected static void ensureTableCreated(String url, String tableName, byte[][] splits) throws SQLException {
        ensureTableCreated(url, tableName, splits, null);
    }

    protected static void ensureTableCreated(String url, String tableName, Long ts) throws SQLException {
        ensureTableCreated(url, tableName, null, ts);
    }

    protected static void ensureTableCreated(String url, String tableName, byte[][] splits, Long ts) throws SQLException {
        String ddl = tableDDLMap.get(tableName);
        createTestTable(url, ddl, splits, ts);
    }

    protected static void createTestTable(String url, String ddl) throws SQLException {
        createTestTable(url, ddl, null, null);
    }

    protected static void createTestTable(String url, String ddl, byte[][] splits, Long ts) throws SQLException {
        assertNotNull(ddl);
        StringBuilder buf = new StringBuilder(ddl);
        if (splits != null) {
            buf.append(" SPLIT ON (");
            for (int i = 0; i < splits.length; i++) {
                buf.append("?,");
            }
            buf.setCharAt(buf.length()-1, ')');
        }
        ddl = buf.toString();
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement stmt = conn.prepareStatement(ddl);
            if (splits != null) {
                for (int i = 0; i < splits.length; i++) {
                    stmt.setBytes(i+1, splits[i]);
                }
            }
            stmt.execute(ddl);
        } catch (TableAlreadyExistsException e) {
        } finally {
            conn.close();
        }
    }
}
