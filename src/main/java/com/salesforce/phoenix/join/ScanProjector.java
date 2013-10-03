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
package com.salesforce.phoenix.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.KeyValueUtil;

public class ScanProjector {    
    private static final String SCAN_PROJECTOR = "scanProjector";    
    private static final byte[] PREFIX_SEPERATOR = Bytes.toBytes(":");
    private static final byte[] PROJECTED_ROW = Bytes.toBytes("_");
    private static final byte[] EMPTY_QUALIFIER = new byte[0];
    
    private final byte[] tablePrefix;

    public static byte[] getRowFamily(byte[] cfPrefix) {
        return ByteUtil.concat(cfPrefix, PREFIX_SEPERATOR);
    }
    
    public static byte[] getRowQualifier() {
        return EMPTY_QUALIFIER;
    }
    
    public static byte[] getPrefixedColumnFamily(byte[] columnFamily, byte[] cfPrefix) {
        return ByteUtil.concat(cfPrefix, PREFIX_SEPERATOR, columnFamily);
    }
    
    public static byte[] getPrefixForTable(TableRef table) {
        if (table.getTableAlias() == null)
            return table.getTable().getName().getBytes();
        
        return Bytes.toBytes(table.getTableAlias());
    }
    
    public ScanProjector(byte[] tablePrefix) {
        this.tablePrefix = tablePrefix;
    }
    
    public static void serializeProjectorIntoScan(Scan scan, ScanProjector projector) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            WritableUtils.writeCompressedByteArray(output, projector.tablePrefix);
            scan.setAttribute(SCAN_PROJECTOR, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    public static ScanProjector deserializeProjectorFromScan(Scan scan) {
        byte[] proj = scan.getAttribute(SCAN_PROJECTOR);
        if (proj == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(proj);
        try {
            DataInputStream input = new DataInputStream(stream);
            byte[] tablePrefix = WritableUtils.readCompressedByteArray(input);
            return new ScanProjector(tablePrefix);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public byte[] getTablePrefix() {
        return this.tablePrefix;
    }
    
    public KeyValue projectRow(KeyValue kv) {
        return KeyValueUtil.newKeyValue(PROJECTED_ROW, getRowFamily(tablePrefix), getRowQualifier(), 
                kv.getTimestamp(), kv.getBuffer(), kv.getRowOffset(), kv.getRowLength());
    }
    
    public KeyValue projectedKeyValue(KeyValue kv) {
        byte[] cf = getPrefixedColumnFamily(kv.getFamily(), tablePrefix);
        return KeyValueUtil.newKeyValue(PROJECTED_ROW, cf, kv.getQualifier(), 
                kv.getTimestamp(), kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }
}
