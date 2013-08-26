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
package com.salesforce.phoenix.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.expression.*;
import com.salesforce.phoenix.filter.*;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.schema.tuple.Tuple;



public class TestUtil {
    private TestUtil() {
    }

    public static final String CF_NAME = "a";
    public static final byte[] CF = Bytes.toBytes(CF_NAME);

    public static final String CF2_NAME = "b";

    public final static String A_VALUE = "a";
    public final static byte[] A = Bytes.toBytes(A_VALUE);
    public final static String B_VALUE = "b";
    public final static byte[] B = Bytes.toBytes(B_VALUE);
    public final static String C_VALUE = "c";
    public final static byte[] C = Bytes.toBytes(C_VALUE);
    public final static String D_VALUE = "d";
    public final static byte[] D = Bytes.toBytes(D_VALUE);
    public final static String E_VALUE = "e";
    public final static byte[] E = Bytes.toBytes(E_VALUE);

    public final static String ROW1 = "00A123122312312";
    public final static String ROW2 = "00A223122312312";
    public final static String ROW3 = "00A323122312312";
    public final static String ROW4 = "00A423122312312";
    public final static String ROW5 = "00B523122312312";
    public final static String ROW6 = "00B623122312312";
    public final static String ROW7 = "00B723122312312";
    public final static String ROW8 = "00B823122312312";
    public final static String ROW9 = "00C923122312312";

    public static final long MILLIS_IN_DAY = 1000 * 60 * 60 * 24;
    public static final String PHOENIX_JDBC_URL = "jdbc:phoenix:localhost;test=true";
    public static final String PHOENIX_CONNECTIONLESS_JDBC_URL = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + PhoenixRuntime.CONNECTIONLESS  + ";test=true";

    public static final String TEST_SCHEMA_FILE_NAME = "config" + File.separator + "test-schema.xml";
    public static final String CED_SCHEMA_FILE_NAME = "config" + File.separator + "schema.xml";
    public static final String ATABLE_NAME = "ATABLE";
    public static final String SUM_DOUBLE_NAME = "SumDoubleTest";
    public static final String ATABLE_SCHEMA_NAME = "";
    public static final String BTABLE_NAME = "BTABLE";
    public static final String STABLE_NAME = "STABLE";
    public static final String STABLE_SCHEMA_NAME = "";
    public static final String GROUPBYTEST_NAME = "GROUPBYTEST";
    public static final String CUSTOM_ENTITY_DATA_FULL_NAME = "CORE.CUSTOM_ENTITY_DATA";
    public static final String CUSTOM_ENTITY_DATA_NAME = "CUSTOM_ENTITY_DATA";
    public static final String CUSTOM_ENTITY_DATA_SCHEMA_NAME = "CORE";
    public static final String HBASE_NATIVE = "HBASE_NATIVE";
    public static final String HBASE_DYNAMIC_COLUMNS = "HBASE_DYNAMIC_COLUMNS";
    public static final String PRODUCT_METRICS_NAME = "PRODUCT_METRICS";
    public static final String PTSDB_NAME = "PTSDB";
    public static final String PTSDB2_NAME = "PTSDB2";
    public static final String PTSDB3_NAME = "PTSDB3";
    public static final String PTSDB_SCHEMA_NAME = "";
    public static final String FUNKY_NAME = "FUNKY_NAMES";
    public static final String MULTI_CF_NAME = "MULTI_CF";
    public static final String MDTEST_NAME = "MDTEST";
    public static final String KEYONLY_NAME = "KEYONLY";
    public static final String TABLE_WITH_SALTING = "TABLE_WITH_SALTING";
    public static final String INDEX_DATA_SCHEMA = "INDEX_TEST";
    public static final String INDEX_DATA_TABLE = "INDEX_DATA_TABLE";

    public static final Properties TEST_PROPERTIES = new Properties();

    public static byte[][] getSplits(String tenantId) {
        return new byte[][] {
            HConstants.EMPTY_BYTE_ARRAY,
            Bytes.toBytes(tenantId + "00A"),
            Bytes.toBytes(tenantId + "00B"),
            Bytes.toBytes(tenantId + "00C"),
            };
    }

    public static void assertRoundEquals(BigDecimal bd1, BigDecimal bd2) {
        bd1 = bd1.round(PDataType.DEFAULT_MATH_CONTEXT);
        bd2 = bd2.round(PDataType.DEFAULT_MATH_CONTEXT);
        if (bd1.compareTo(bd2) != 0) {
            fail("expected:<" + bd1 + "> but was:<" + bd2 + ">");
        }
    }

    public static BigDecimal computeAverage(double sum, long count) {
        return BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(count), PDataType.DEFAULT_MATH_CONTEXT);
    }

    public static BigDecimal computeAverage(long sum, long count) {
        return BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(count), PDataType.DEFAULT_MATH_CONTEXT);
    }

    public static Expression constantComparison(CompareOp op, PColumn c, Object o) {
        return  new ComparisonExpression(op, Arrays.<Expression>asList(new KeyValueColumnExpression(c), LiteralExpression.newConstant(o)));
    }

    public static Expression kvColumn(PColumn c) {
        return new KeyValueColumnExpression(c);
    }

    public static Expression pkColumn(PColumn c, List<PColumn> columns) {
        return new RowKeyColumnExpression(c, new RowKeyValueAccessor(columns, columns.indexOf(c)));
    }

    public static Expression constantComparison(CompareOp op, Expression e, Object o) {
        return  new ComparisonExpression(op, Arrays.asList(e, LiteralExpression.newConstant(o)));
    }

    public static Expression columnComparison(CompareOp op, PColumn c1, PColumn c2) {
        return  new ComparisonExpression(op, Arrays.<Expression>asList(new KeyValueColumnExpression(c1), new KeyValueColumnExpression(c2)));
    }

    public static SingleKeyValueComparisonFilter singleKVFilter(Expression e) {
        return  new SingleCQKeyValueComparisonFilter(e);
    }

    public static RowKeyComparisonFilter rowKeyFilter(Expression e) {
        return  new RowKeyComparisonFilter(e, QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES);
    }

    public static MultiKeyValueComparisonFilter multiKVFilter(Expression e) {
        return  new MultiCQKeyValueComparisonFilter(e);
    }

    public static Expression and(Expression... expressions) {
        return new AndExpression(Arrays.asList(expressions));
    }

    public static Expression not(Expression expression) {
        return new NotExpression(expression);
    }
    
    public static Expression or(Expression... expressions) {
        return new OrExpression(Arrays.asList(expressions));
    }

    public static Expression in(Expression... expressions) throws SQLException {
        return new InListExpression(Arrays.asList(expressions));
    }

    public static Expression in(Expression e, PDataType childType, Object... literals) throws SQLException {
        List<Expression> expressions = new ArrayList<Expression>(literals.length + 1);
        expressions.add(e);
        for (Object o : literals) {
            expressions.add(LiteralExpression.newConstant(o, childType));
        }
        return new InListExpression(expressions);
    }

    public static void assertDegenerate(StatementContext context) {
        Scan scan = context.getScan();
        assertDegenerate(scan);
    }

    public static void assertDegenerate(Scan scan) {
        assertNull(scan.getFilter());
        assertArrayEquals(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStartRow());
        assertArrayEquals(KeyRange.EMPTY_RANGE.getLowerRange(), scan.getStopRow());
        assertEquals(null,scan.getFilter());
    }

    public static void assertEmptyScanKey(Scan scan) {
        assertNull(scan.getFilter());
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, scan.getStartRow());
        assertArrayEquals(ByteUtil.EMPTY_BYTE_ARRAY, scan.getStopRow());
        assertEquals(null,scan.getFilter());
    }

    /**
     * Does a deep comparison of two Results, down to the byte arrays.
     * @param res1 first result to compare
     * @param res2 second result to compare
     * @throws Exception Every difference is throwing an exception
     */
    public static void compareTuples(Tuple res1, Tuple res2)
        throws Exception {
      if (res2 == null) {
        throw new Exception("There wasn't enough rows, we stopped at "
            + res1);
      }
      if (res1.size() != res2.size()) {
        throw new Exception("This row doesn't have the same number of KVs: "
            + res1.toString() + " compared to " + res2.toString());
      }
      for (int i = 0; i < res1.size(); i++) {
          KeyValue ourKV = res1.getValue(i);
          KeyValue replicatedKV = res2.getValue(i);
          if (!ourKV.equals(replicatedKV) ||
              !Bytes.equals(ourKV.getValue(), replicatedKV.getValue())) {
              throw new Exception("This result was different: "
                  + res1.toString() + " compared to " + res2.toString());
        }
      }
    }

    public static void clearMetaDataCache(Connection conn) throws Throwable {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        HTableInterface htable = pconn.getQueryServices().getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME);
        htable.coprocessorExec(MetaDataProtocol.class, HConstants.EMPTY_START_ROW,
                HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataProtocol, Void>() {
            @Override
            public Void call(MetaDataProtocol instance) throws IOException {
              instance.clearCache();
              return null;
            }
          });
    }


}
