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

import static com.salesforce.phoenix.util.SchemaUtil.getVarChars;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.PDataType;


public class MetaDataUtil {
    // Given the encoded integer representing the phoenix version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte 
    // patch version.
    public static int decodePhoenixVersion(long version) {
        return (int) ((version << Byte.SIZE * 3) >>> Byte.SIZE * 5);
    }

    // Given the encoded integer representing the client hbase version in the encoded version value.
    // The second byte in int would be the major version, 3rd byte minor version, and 4th byte 
    // patch version.
    public static int decodeHBaseVersion(long version) {
        return (int) (version >>> Byte.SIZE * 5);
    }

    public static long encodeHBaseAndPhoenixVersions(String hbaseVersion) {
        return (((long) encodeVersion(hbaseVersion)) << (Byte.SIZE * 5)) |
                (((long) encodeVersion(MetaDataProtocol.PHOENIX_MAJOR_VERSION, MetaDataProtocol.PHOENIX_MINOR_VERSION,
                        MetaDataProtocol.PHOENIX_PATCH_NUMBER)) << (Byte.SIZE * 2));
    }

    // Encode a version string in the format of "major.minor.patch" into an integer.
    public static int encodeVersion(String version) {
        String[] versionParts = version.split("[-\\.]");
        return encodeVersion(versionParts[0], versionParts.length > 1 ? versionParts[1] : null, versionParts.length > 2 ? versionParts[2] : null);
    }

    // Encode the major as 2nd byte in the int, minor as the first byte and patch as the last byte.
    public static int encodeVersion(String major, String minor, String patch) {
        return encodeVersion(major == null ? 0 : Integer.parseInt(major), minor == null ? 0 : Integer.parseInt(minor), 
                        patch == null ? 0 : Integer.parseInt(patch));
    }

    public static int encodeVersion(int major, int minor, int patch) {
        int version = 0;
        version |= (major << Byte.SIZE * 2);
        version |= (minor << Byte.SIZE);
        version |= patch;
        return version;
    }

    public static void getSchemaAndTableName(List<Mutation> tableMetadata, byte[][] rowKeyMetaData) {
        Mutation m = getTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 2, rowKeyMetaData);
    }
    
    public static byte[] getParentTableName(List<Mutation> tableMetadata) {
        if (tableMetadata.size() == 1) {
            return null;
        }
        byte[][] rowKeyMetaData = new byte[2][];
        getSchemaAndTableName(tableMetadata, rowKeyMetaData);
        byte[] tableName = rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
        Mutation m = getParentTableHeaderRow(tableMetadata);
        getVarChars(m.getRow(), 2, rowKeyMetaData);
        if (Bytes.compareTo(tableName, rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX]) == 0) {
            return null;
        }
        return rowKeyMetaData[PhoenixDatabaseMetaData.TABLE_NAME_INDEX];
    }
    
    public static long getSequenceNumber(Mutation tableMutation) {
        List<KeyValue> kvs = tableMutation.getFamilyMap().get(PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES);
        if (kvs != null) {
            for (KeyValue kv : kvs) { // list is not ordered, so search. TODO: we could potentially assume the position
                if (Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(), PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES, 0, PhoenixDatabaseMetaData.TABLE_SEQ_NUM_BYTES.length) == 0) {
                    return PDataType.LONG.getCodec().decodeLong(kv.getBuffer(), kv.getValueOffset(), null);
                }
            }
        }
        throw new IllegalStateException();
    }
    
    public static long getSequenceNumber(List<Mutation> tableMetaData) {
        return getSequenceNumber(getTableHeaderRow(tableMetaData));
    }
    
    public static long getParentSequenceNumber(List<Mutation> tableMetaData) {
        return getSequenceNumber(getParentTableHeaderRow(tableMetaData));
    }
    
    public static Mutation getTableHeaderRow(List<Mutation> tableMetaData) {
        return tableMetaData.get(0);
    }

    public static Mutation getParentTableHeaderRow(List<Mutation> tableMetaData) {
        return tableMetaData.get(tableMetaData.size()-1);
    }

    public static long getClientTimeStamp(List<Mutation> tableMetadata) {
        Mutation m = tableMetadata.get(0);
        return getClientTimeStamp(m);
    }    

    public static long getClientTimeStamp(Mutation m) {
        Collection<List<KeyValue>> kvs = m.getFamilyMap().values();
        // Empty if Mutation is a Delete
        // TODO: confirm that Delete timestamp is reset like Put
        return kvs.isEmpty() ? m.getTimeStamp() : kvs.iterator().next().get(0).getTimestamp();
    }    

    public static byte[] getParentLinkKey(String schemaName, String tableName, String indexName) {
        return ByteUtil.concat(schemaName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(schemaName), QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(tableName), QueryConstants.SEPARATOR_BYTE_ARRAY, QueryConstants.SEPARATOR_BYTE_ARRAY, Bytes.toBytes(indexName));
    }


}
