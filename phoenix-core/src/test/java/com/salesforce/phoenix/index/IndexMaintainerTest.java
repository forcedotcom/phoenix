package com.salesforce.phoenix.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.salesforce.hbase.index.ValueGetter;
import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.end2end.index.IndexTestUtil;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.SchemaUtil;

public class IndexMaintainerTest  extends BaseConnectionlessQueryTest {
    private static final String DEFAULT_SCHEMA_NAME = "";
    private static final String DEFAULT_TABLE_NAME = "rkTest";
    
    @Before
    public void beforeTest() throws Exception {
        stopServer();
        startServer(getUrl());
    }
    
    
    private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns, Object[] values) throws Exception {
        testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns, values, "", "", "");
    }

    private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns, Object[] values, String includeColumns) throws Exception {
        testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns, values, includeColumns, "", "");
    }

    private void testIndexRowKeyBuilding(String dataColumns, String pk, String indexColumns, Object[] values, String includeColumns, String dataProps, String indexProps) throws Exception {
        testIndexRowKeyBuilding(DEFAULT_SCHEMA_NAME, DEFAULT_TABLE_NAME, dataColumns, pk, indexColumns, values, "", dataProps, indexProps);
    }

    private static ValueGetter newValueGetter(final Map<ColumnReference, byte[]> valueMap) {
        return new ValueGetter() {

            @Override
            public ImmutableBytesPtr getLatestValue(ColumnReference ref) {
                return new ImmutableBytesPtr(valueMap.get(ref));
            }
            
        };
    }
    
    private void testIndexRowKeyBuilding(String schemaName, String tableName, String dataColumns, String pk, String indexColumns, Object[] values, String includeColumns, String dataProps, String indexProps) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName) ;
        conn.createStatement().execute("CREATE TABLE " + fullTableName + "(" + dataColumns + " CONSTRAINT pk PRIMARY KEY (" + pk + "))  " + (dataProps.isEmpty() ? "" : dataProps) );
        try {
            conn.createStatement().execute("CREATE INDEX idx ON " + fullTableName + "(" + indexColumns + ") " + (includeColumns.isEmpty() ? "" : "INCLUDE (" + includeColumns + ") ") + (indexProps.isEmpty() ? "" : indexProps));
            PTable table = conn.unwrap(PhoenixConnection.class).getPMetaData().getTable(SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),SchemaUtil.normalizeIdentifier(tableName)));
            PTable index = conn.unwrap(PhoenixConnection.class).getPMetaData().getTable(SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),SchemaUtil.normalizeIdentifier("idx")));
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            table.getIndexMaintainers(ptr);
            List<IndexMaintainer> c1 = IndexMaintainer.deserialize(ptr);
            assertEquals(1,c1.size());
            IndexMaintainer im1 = c1.get(0);
            
            StringBuilder buf = new StringBuilder("UPSERT INTO " + fullTableName  + " VALUES(");
            for (int i = 0; i < values.length; i++) {
                buf.append("?,");
            }
            buf.setCharAt(buf.length()-1, ')');
            PreparedStatement stmt = conn.prepareStatement(buf.toString());
            for (int i = 0; i < values.length; i++) {
                stmt.setObject(i+1, values[i]);
            }
            stmt.execute();
            	Iterator<Pair<byte[],List<KeyValue>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
            List<KeyValue> dataKeyValues = iterator.next().getSecond();
            Map<ColumnReference,byte[]> valueMap = Maps.newHashMapWithExpectedSize(dataKeyValues.size());
            ImmutableBytesWritable rowKeyPtr = new ImmutableBytesWritable(dataKeyValues.get(0).getRow());
            Put dataMutation = new Put(rowKeyPtr.copyBytes());
            for (KeyValue kv : dataKeyValues) {
                valueMap.put(new ColumnReference(kv.getFamily(),kv.getQualifier()), kv.getValue());
                dataMutation.add(kv);
            }
            ValueGetter valueGetter = newValueGetter(valueMap);
            
            List<Mutation> indexMutations = IndexTestUtil.generateIndexData(index, table, dataMutation, ptr);
            assertEquals(1,indexMutations.size());
            assertTrue(indexMutations.get(0) instanceof Put);
            Mutation indexMutation = indexMutations.get(0);
            ImmutableBytesWritable indexKeyPtr = new ImmutableBytesWritable(indexMutation.getRow());
            
            ptr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
            byte[] mutablelndexRowKey = im1.buildRowKey(valueGetter, ptr);
            byte[] immutableIndexRowKey = indexKeyPtr.copyBytes();
            assertArrayEquals(immutableIndexRowKey, mutablelndexRowKey);
            
            for (ColumnReference ref : im1.getCoverededColumns()) {
                valueMap.get(ref);
            }
        } finally {
            try {
                conn.createStatement().execute("DROP TABLE " + fullTableName);
            } finally {
                conn.close();
            }
        }
    }

    @Test
    public void testRowKeyVarOnlyIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL", "k1,k2", "k2, k1", new Object [] {"a",1.1});
    }
 
    @Test
    public void testVarFixedndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2, k1", new Object [] {"a",1.1});
    }
 
    
    @Test
    public void testCompositeRowKeyVarFixedIndex() throws Exception {
        // TODO: using 1.1 for INTEGER didn't give error
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2, k1", new Object [] {"a",1});
    }
 
    @Test
    public void testCompositeRowKeyVarFixedAtEndIndex() throws Exception {
        // Forces trailing zero in index key for fixed length
        for (int i = 0; i < 10; i++) {
            testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, k3 VARCHAR, v VARCHAR", "k1,k2,k3", "k1, k3, k2", new Object [] {"a",i, "b"});
        }
    }
 
   @Test
    public void testSingleKeyValueIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1", "v", new Object [] {"a",1,"b"});
    }
 
    @Test
    public void testMultiKeyValueIndex() throws Exception {
        testIndexRowKeyBuilding("k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT", "k1, k2", "v2, k2, v1", new Object [] {"a",1,2.2,"bb"});
    }
 
    @Test
    public void testMultiKeyValueCoveredIndex() throws Exception {
        testIndexRowKeyBuilding("k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2", "v2, k2, v1", new Object [] {"a",1,2.2,"bb"}, "v3, v4");
    }
 
    @Test
    public void testSingleKeyValueDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1", "v DESC", new Object [] {"a",1,"b"});
    }
 
    @Test
    public void testCompositeRowKeyVarFixedDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1,k2", "k2 DESC, k1", new Object [] {"a",1});
    }
 
    @Test
    public void testCompositeRowKeyTimeIndex() throws Exception {
        long timeInMillis = System.currentTimeMillis();
        long timeInNanos = System.nanoTime();
        Timestamp ts = new Timestamp(timeInMillis);
        ts.setNanos((int) (timeInNanos % 1000000000));
        testIndexRowKeyBuilding("ts1 DATE NOT NULL, ts2 TIME NOT NULL, ts3 TIMESTAMP NOT NULL", "ts1,ts2,ts3", "ts2, ts1", new Object [] {new Date(timeInMillis), new Time(timeInMillis), ts});
    }
 
    @Test
    public void testCompositeRowKeyBytesIndex() throws Exception {
        long timeInMillis = System.currentTimeMillis();
        long timeInNanos = System.nanoTime();
        Timestamp ts = new Timestamp(timeInMillis);
        ts.setNanos((int) (timeInNanos % 1000000000));
        testIndexRowKeyBuilding("b1 BINARY(3) NOT NULL, v VARCHAR", "b1,v", "v, b1", new Object [] {new byte[] {41,42,43}, "foo"});
    }
 
    @Test
    public void testCompositeDescRowKeyVarFixedDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1});
    }
 
    @Test
    public void testCompositeDescRowKeyVarDescIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1.1,"b"});
    }
 
    @Test
    public void testCompositeDescRowKeyVarAscIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 DECIMAL NOT NULL, v VARCHAR", "k1, k2 DESC", "k2, k1", new Object [] {"a",1.1,"b"});
    }
 
    @Test
    public void testCompositeDescRowKeyVarFixedDescSaltedIndex() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1}, "", "", "SALT_BUCKETS=4");
    }
 
    @Test
    public void testCompositeDescRowKeyVarFixedDescSaltedIndexSaltedTable() throws Exception {
        testIndexRowKeyBuilding("k1 VARCHAR, k2 INTEGER NOT NULL, v VARCHAR", "k1, k2 DESC", "k2 DESC, k1", new Object [] {"a",1}, "", "SALT_BUCKETS=3", "SALT_BUCKETS=4");
    }
 
    @Test
    public void testMultiKeyValueCoveredSaltedIndex() throws Exception {
        testIndexRowKeyBuilding("k1 CHAR(1) NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, v2 CHAR(2), v3 BIGINT, v4 CHAR(10)", "k1, k2", "v2 DESC, k2 DESC, v1", new Object [] {"a",1,2.2,"bb"}, "v3, v4", "", "SALT_BUCKETS=4");
    }
 

}
