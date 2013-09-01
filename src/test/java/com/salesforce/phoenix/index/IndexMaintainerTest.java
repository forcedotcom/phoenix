package com.salesforce.phoenix.index;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.Test;

import com.salesforce.hbase.index.builder.covered.ColumnReference;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.PhoenixRuntime;

public class IndexMaintainerTest  extends BaseConnectionlessQueryTest {

    @Before
    public void beforeTest() throws Exception {
        stopServer();
        startServer(getUrl());
    }
    
    @Test
    public void testCompositeRowKeyOnlyIndex1() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE rkTest (k1 VARCHAR, k2 DECIMAL CONSTRAINT pk PRIMARY KEY (k1,k2)) IMMUTABLE_ROWS=true");
        conn.createStatement().execute("CREATE INDEX rkIdx ON rkTest (k2, k1)");
        PTable table = conn.unwrap(PhoenixConnection.class).getPMetaData().getSchema("").getTable("RKTEST");
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        table.getIndexMaintainers(ByteUtil.EMPTY_BYTE_ARRAY, ptr);
        List<IndexMaintainer> c1 = IndexMaintainer.deserialize(ptr.get());
        assertEquals(1,c1.size());
        IndexMaintainer im1 = c1.get(0);
        
        conn.createStatement().execute("UPSERT INTO rkTest VALUES('a',1.1)");
        Iterator<Pair<byte[],List<KeyValue>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        List<KeyValue> dataKeyValues = iterator.next().getSecond();
        ImmutableBytesWritable rowKeyPtr = new ImmutableBytesWritable(dataKeyValues.get(0).getRow());
        List<KeyValue> indexKeyValues = iterator.next().getSecond();
        ImmutableBytesWritable indexKeyPtr = new ImmutableBytesWritable(indexKeyValues.get(0).getRow());
        
        Map<ColumnReference,byte[]> valueMap = Collections.emptyMap();
        ptr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
        byte[] indexRowKey = im1.buildRowKey(valueMap, ptr);
        assertArrayEquals(indexKeyPtr.copyBytes(), indexRowKey);
    }
}
