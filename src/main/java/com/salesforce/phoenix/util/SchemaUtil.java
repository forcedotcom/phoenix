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

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_TABLE_NAME;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;



/**
 * 
 * Static class for various schema-related utilities
 *
 * @author jtaylor
 * @since 0.1
 */
public class SchemaUtil {
    private static final int VAR_LENGTH_ESTIMATE = 10;
    
    public static final DataBlockEncoding DEFAULT_DATA_BLOCK_ENCODING = DataBlockEncoding.FAST_DIFF;
    /**
     * May not be instantiated
     */
    private SchemaUtil() {
    }

    /**
     * Join srcRow KeyValues to dstRow KeyValues.  For single-column mode, in the case of multiple
     * column families (which can happen for our conceptual PK to PK joins), we always use the
     * column family from the first dstRow.  TODO: we'll likely need a different coprocessor to
     * handle the PK join case.
     * @param srcRow the source list of KeyValues to join with dstRow KeyValues.  The key will be
     * changed upon joining to match the dstRow key.  The list may not be empty and is left unchanged.
     * @param dstRow the list of KeyValues to which the srcRows are joined.  The list is modified in
     * place and may not be empty.
     */
    public static void joinColumns(List<KeyValue> srcRow, List<KeyValue> dstRow) {
        assert(!dstRow.isEmpty());
        KeyValue dst = dstRow.get(0);
        byte[] dstBuf = dst.getBuffer();
        int dstKeyOffset = dst.getRowOffset();
        int dstKeyLength = dst.getRowLength();
        // Combine columns from both rows
        // The key for the cached KeyValues are modified to match the other key.
        for (KeyValue srcValue : srcRow) {
            byte[] srcBuf = srcValue.getBuffer();
            byte type = srcValue.getType();
            KeyValue dstValue = new KeyValue(dstBuf, dstKeyOffset, dstKeyLength,
                srcBuf, srcValue.getFamilyOffset(), srcValue.getFamilyLength(),
                srcBuf, srcValue.getQualifierOffset(), srcValue.getQualifierLength(),
                HConstants.LATEST_TIMESTAMP, KeyValue.Type.codeToType(type),
                srcBuf, srcValue.getValueOffset(), srcValue.getValueLength());
            dstRow.add(dstValue);
        }
        // Put KeyValues in proper sort order
        // TODO: our tests need this, but otherwise would this be required?
        Collections.sort(dstRow, KeyValue.COMPARATOR);
   }
    
    /**
     * Get the column value of a row.
     * @param result the Result return from iterating through scanner results.
     * @param fam the column family 
     * @param col the column qualifier
     * @param pos the column position
     * @param value updated in place to the bytes representing the column value
     * @return true if the column exists and value was set and false otherwise.
     */
    public static boolean getValue(Result result, byte[] fam, byte[] col, int pos, ImmutableBytesWritable value) {
        KeyValue keyValue = ResultUtil.getColumnLatest(result, fam, col);
        if (keyValue != null) {
            value.set(keyValue.getBuffer(),keyValue.getValueOffset(),keyValue.getValueLength());
            return true;
        }
        return false;
    }

    /**
     * Concatenate two PColumn arrays
     * @param first first array
     * @param second second array
     * @return new combined array
     */
    public static PColumn[] concat(PColumn[] first, PColumn[] second) {
        PColumn[] result = new PColumn[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
    
    public static boolean isPKColumn(PColumn column) {
        return column.getFamilyName() == null;
    }
    
    /**
     * Estimate the max key length in bytes of the PK for a given table
     * @param table the table
     * @return the max PK length
     */
    public static int estimateKeyLength(PTable table) {
        int maxKeyLength = 0;
        // Calculate the max length of a key (each part must currently be of a fixed width)
        int i = 0;
        List<PColumn> columns = table.getPKColumns();
        while (i < columns.size()) {
            PColumn keyColumn = columns.get(i++);
            Integer maxLength = keyColumn.getMaxLength();
            maxKeyLength += (maxLength == null) ? VAR_LENGTH_ESTIMATE : maxLength;
        }
        return maxKeyLength;
    }

    /**
     * Normalize an identifier. If name is surrounded by double quotes,
     * it is used as-is, otherwise the name is upper caased.
     * @param name the parsed identifier
     * @return the normalized identifier
     */
    public static String normalizeIdentifier(String name) {
        if (name == null) {
            return name;
        }
        if (isCaseSensitive(name)) {
            // Don't upper case if in quotes
            return name.substring(1, name.length()-1);
        }
        return name.toUpperCase();
    }

    public static boolean isCaseSensitive(String name) {
        return name.length() > 0 && name.charAt(0)=='"';
    }
    
    public static <T> List<T> concat(List<T> l1, List<T> l2) {
        int size1 = l1.size();
        if (size1 == 0) {
            return l2;
        }
        int size2 = l2.size();
        if (size2 == 0) {
            return l1;
        }
        List<T> l3 = new ArrayList<T>(size1 + size2);
        l3.addAll(l1);
        l3.addAll(l2);
        return l3;
    }

    /**
     * Get the key used in the Phoenix metadata row for a table definition
     * @param schemaName
     * @param tableName
     */
    public static byte[] getTableKey(byte[] schemaName, byte[] tableName) {
        return ByteUtil.concat(schemaName, QueryConstants.SEPARATOR_BYTE_ARRAY, tableName, QueryConstants.SEPARATOR_BYTE_ARRAY);
    }

    public static byte[] getTableKey(String schemaName, String tableName) {
        return ByteUtil.concat(schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName), QueryConstants.SEPARATOR_BYTE_ARRAY);
    }

    public static String getTableDisplayName(String schemaName, String tableName) {
        return Bytes.toStringBinary(getTableName(schemaName,tableName));
    }

    public static String getTableDisplayName(byte[] schemaName, byte[] tableName) {
        return Bytes.toStringBinary(getTableName(schemaName,tableName));
    }

    public static String getTableDisplayName(byte[] tableName) {
        return Bytes.toStringBinary(tableName);
    }

    public static String getColumnDisplayName(String schemaName, String tableName, String familyName, String columnName) {
        return Bytes.toStringBinary(getColumnName(
                StringUtil.toBytes(schemaName), StringUtil.toBytes(tableName), 
                StringUtil.toBytes(familyName), StringUtil.toBytes(columnName)));
    }

    /**
     * Get the HTable name for a given schemaName and tableName
     * @param tableName
     */
    public static byte[] getTableName(String tableName) {
        return getTableName(null, tableName);
    }

    public static byte[] getTableName(String schemaName, String tableName) {
        if (schemaName == null || schemaName.length() == 0) {
            return getTableName(StringUtil.toBytes(tableName));
        }
        return getTableName(StringUtil.toBytes(schemaName),StringUtil.toBytes(tableName));
    }

    public static byte[] getTableName(byte[] tableName) {
        return getTableName(null, tableName);
    }

    public static byte[] getTableName(byte[] schemaName, byte[] tableName) {
        return concatTwoNames(schemaName, tableName);
    }

    public static byte[] getColumnName(byte[] schemaName, byte[] tableName, byte[] familyName, byte[] columnName) {
        byte[] tableNamePart = concatTwoNames(schemaName, tableName);
        byte[] columnNamePart = concatTwoNames(familyName, columnName);
        return concatTwoNames(tableNamePart, columnNamePart);
    }

    private static byte[] concatTwoNames(byte[] nameOne, byte[] nameTwo) {
        if (nameOne == null || nameOne.length == 0) {
            return nameTwo;
        } else if ((nameTwo == null || nameTwo.length == 0)) {
            return nameOne;
        } else {
            return ByteUtil.concat(nameOne, QueryConstants.NAME_SEPARATOR_BYTES, nameTwo);
        }
    }

    public static int getVarCharLength(byte[] buf, int keyOffset, int maxLength) {
        int length=0;
        while (length < maxLength && buf[keyOffset+length] != QueryConstants.SEPARATOR_BYTE) {
            length++;
        }
        return length;
    }
    
    public static int getVarChars(byte[] rowKey, byte[][] rowKeyMetadata) {
        return getVarChars(rowKey, 0, rowKey.length, 0, rowKeyMetadata);
    }
    
    public static int getVarChars(byte[] rowKey, int colMetaDataLength, byte[][] colMetaData) {
        return getVarChars(rowKey, 0, rowKey.length, 0, colMetaDataLength, colMetaData);
    }
    
    public static int getVarChars(byte[] rowKey, int keyOffset, int keyLength, int colMetaDataOffset, byte[][] colMetaData) {
        return getVarChars(rowKey, keyOffset, keyLength, colMetaDataOffset, colMetaData.length, colMetaData);
    }
    
    public static int getVarChars(byte[] rowKey, int keyOffset, int keyLength, int colMetaDataOffset, int colMetaDataLength, byte[][] colMetaData) {
        int i, offset = keyOffset;
        for (i = colMetaDataOffset; i < colMetaDataLength && keyLength > 0; i++) {
            int length = getVarCharLength(rowKey, offset, keyLength);
            byte[] b = new byte[length];
            System.arraycopy(rowKey, offset, b, 0, length);
            offset += length + 1;
            keyLength -= length + 1;
            colMetaData[i] = b;
        }
        return i;
    }
    
    public static String findExistingColumn(PTable table, List<PColumn> columns) {
        for (PColumn column : columns) {
            PName familyName = column.getFamilyName();
            if (familyName == null) {
                try {
                    return table.getPKColumn(column.getName().getString()).getName().getString();
                } catch (ColumnNotFoundException e) {
                    continue;
                }
            } else {
                try {
                    return table.getColumnFamily(familyName.getString()).getColumn(column.getName().getString()).getName().getString();
                } catch (ColumnFamilyNotFoundException e) {
                    continue; // Shouldn't happen
                } catch (ColumnNotFoundException e) {
                    continue;
                }
            }
        }
        return null;
    }

    public static void initMetaData(ConnectionQueryServices services, String url, Properties props) throws SQLException {
//        HBaseAdmin admin = null;
//        try {
//            admin = new HBaseAdmin(services.getConfig());
//            admin.disableTable("SYSTEM.TABLE");
//            admin.deleteTable("SYSTEM.TABLE");
//        } catch (IOException e) {
//            throw new SQLException(e);
//        }
        // Use new connection with minimum SCN value
        props = new Properties(props);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_TABLE_TIMESTAMP+1));
        PhoenixConnection metaConnection = new PhoenixConnection(services, url, props, PMetaDataImpl.EMPTY_META_DATA);
        try {
            Statement metaStatement = metaConnection.createStatement();
            metaStatement.executeUpdate(QueryConstants.CREATE_METADATA);
        } catch (TableAlreadyExistsException e) {
        } finally {
            metaConnection.close();
        }
    }

    public static String toString(byte[][] values) {
        if (values == null) {
            return "null";
        }
        StringBuilder buf = new StringBuilder("[");
        for (byte[] value : values) {
            buf.append(Bytes.toStringBinary(value));
            buf.append(',');
        }
        buf.setCharAt(buf.length()-1, ']');
        return buf.toString();
    }

    public static String toString(PDataType type, byte[] value) {
        boolean isString = type.isCoercibleTo(PDataType.VARCHAR);
        return isString ? ("'" + type.toObject(value).toString() + "'") : type.toObject(value).toString();
    }

    public static byte[] getEmptyColumnFamily(List<PColumnFamily> families) {
        return families.isEmpty() ? QueryConstants.EMPTY_COLUMN_BYTES : families.get(0).getName().getBytes();
    }

    public static boolean isMetaTable(byte[] tableName) {
        return Bytes.compareTo(tableName, TYPE_TABLE_NAME) == 0;
    }
}
