package com.salesforce.phoenix.schema;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.SchemaUtil;

public class RowKeyValueAccessorTest  extends BaseConnectionlessQueryTest  {

    public RowKeyValueAccessorTest() {
    }

    private void assertExpectedRowKeyValue(String dataColumns, String pk, Object[] values, int index) throws Exception {
        assertExpectedRowKeyValue(dataColumns,pk,values,index,"");
    }
    
    private void assertExpectedRowKeyValue(String dataColumns, String pk, Object[] values, int index, String dataProps) throws Exception {
        String schemaName = "";
        String tableName = "T";
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName) ;
        conn.createStatement().execute("CREATE TABLE " + fullTableName + "(" + dataColumns + " CONSTRAINT pk PRIMARY KEY (" + pk + "))  " + (dataProps.isEmpty() ? "" : dataProps) );
        PTable table = conn.unwrap(PhoenixConnection.class).getPMetaData().getTable(SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(schemaName),SchemaUtil.normalizeIdentifier(tableName)));
        conn.close();
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
        KeyValue keyValue = dataKeyValues.get(0);
        
        List<PColumn> pkColumns = table.getPKColumns();
        RowKeyValueAccessor accessor = new RowKeyValueAccessor(pkColumns, 3);
        int offset = accessor.getOffset(keyValue.getBuffer(), keyValue.getRowOffset());
        int length = accessor.getLength(keyValue.getBuffer(), offset, keyValue.getOffset()+keyValue.getLength());
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(keyValue.getBuffer(), offset, length);
        
        PDataType dataType = pkColumns.get(index).getDataType();
        Object expectedObject = dataType.toObject(values[index], PDataType.fromLiteral(values[index]));
        dataType.coerceBytes(ptr, dataType, pkColumns.get(index).getColumnModifier(), null);
        Object actualObject = dataType.toObject(ptr);
        assertEquals(expectedObject, actualObject);
    }
    
    @Test
    public void testFixedLengthValueAtEnd() throws Exception {
        assertExpectedRowKeyValue("n VARCHAR NOT NULL, s CHAR(1) NOT NULL, y SMALLINT NOT NULL, o BIGINT NOT NULL", "n,s,y DESC,o DESC", new Object[] {"Abbey","F",2012,253}, 3);
    }
}
