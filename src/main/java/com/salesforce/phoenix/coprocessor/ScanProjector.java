package com.salesforce.phoenix.coprocessor;

import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.ImmutableBytesPtr;
import com.salesforce.phoenix.util.KeyValueUtil;

public class ScanProjector {
    
    public enum ProjectionType {TABLE, CF, CQ};
    
    private static final byte[] SEPERATOR = Bytes.toBytes(":");
    
    private final ProjectionType type;
    private final byte[] tablePrefix;
    private final Map<ImmutableBytesPtr, byte[]> cfProjectionMap;
    private final Map<ImmutableBytesPtr, Map<ImmutableBytesPtr, Pair<byte[], byte[]>>> cqProjectionMap;
    
    private ScanProjector(byte[] tablePrefix) {
        this.type = ProjectionType.TABLE;
        this.tablePrefix = tablePrefix;
        this.cfProjectionMap = null;
        this.cqProjectionMap = null;
    }
    
    public static ScanProjector deserializeProjectorFromScan(Scan scan) {
        return new ScanProjector(null);
    }
    
    public KeyValue getProjectedKeyValue(KeyValue kv) {
        if (type == ProjectionType.TABLE) {
            byte[] cf = ByteUtil.concat(tablePrefix, SEPERATOR, kv.getFamily());
            return KeyValueUtil.newKeyValue(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(), 
                    cf, kv.getQualifier(), kv.getTimestamp(), kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        }
        
        if (type == ProjectionType.CF) {
            byte[] cf = cfProjectionMap.get(new ImmutableBytesPtr(kv.getFamily()));
            if (cf == null)
                return kv;
            return KeyValueUtil.newKeyValue(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(), 
                    cf, kv.getQualifier(), kv.getTimestamp(), kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
        }
        
        Map<ImmutableBytesPtr, Pair<byte[], byte[]>> map = cqProjectionMap.get(new ImmutableBytesPtr(kv.getFamily()));
        if (map == null)
            return kv;
        
        Pair<byte[], byte[]> col = map.get(new ImmutableBytesPtr(kv.getQualifier()));
        if (col == null)
            return kv;
        
        return KeyValueUtil.newKeyValue(kv.getBuffer(), kv.getKeyOffset(), kv.getKeyLength(), 
                col.getFirst(), col.getSecond(), kv.getTimestamp(), kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
    }
}
