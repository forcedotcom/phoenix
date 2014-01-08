package com.salesforce.phoenix.schema;

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.CACHE_SIZE_BYTES;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.INCREMENT_BY_BYTES;
import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.coprocessor.SequenceRegionObserver;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.SchemaUtil;

public class Sequence {
    private static final Long AMOUNT = Long.valueOf(0L);
    // Pre-compute index of sequence key values to prevent binary search
    private static final KeyValue CURRENT_VALUE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CURRENT_VALUE_BYTES);
    private static final KeyValue INCREMENT_BY_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, INCREMENT_BY_BYTES);
    private static final KeyValue CACHE_SIZE_KV = KeyValue.createFirstOnRow(ByteUtil.EMPTY_BYTE_ARRAY, SEQUENCE_FAMILY_BYTES, CACHE_SIZE_BYTES);
    private static final List<KeyValue> SEQUENCE_KV_COLUMNS = Arrays.<KeyValue>asList(
            CURRENT_VALUE_KV,
            INCREMENT_BY_KV,
            CACHE_SIZE_KV
            );
    static {
        Collections.sort(SEQUENCE_KV_COLUMNS, KeyValue.COMPARATOR);
    }
    private static final int CURRENT_VALUE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CURRENT_VALUE_KV);
    private static final int INCREMENT_BY_INDEX = SEQUENCE_KV_COLUMNS.indexOf(INCREMENT_BY_KV);
    private static final int CACHE_SIZE_INDEX = SEQUENCE_KV_COLUMNS.indexOf(CACHE_SIZE_KV);

    private static final int SEQUENCE_KEY_VALUES = SEQUENCE_KV_COLUMNS.size();
    private static final EmptySequenceCacheException EMPTY_SEQUENCE_CACHE_EXCEPTION = new EmptySequenceCacheException();
    
    private final SequenceKey key;
    private final ReentrantLock lock;
    private List<SequenceValue> values;
    
    public Sequence(SequenceKey key) {
        if (key == null) throw new NullPointerException();
        this.key = key;
        this.lock = new ReentrantLock();
    }

    private SequenceValue getSequenceValue(long timestamp) {
        if (values == null) {
            return null;
        }
        int i = values.size()-1;
        while (i >= 0 && values.get(i).timestamp >= timestamp) {
            i--;
        }
        if (i < 0) {
            return null;
        }
        SequenceValue value = values.get(i);
        return value;
    }
    
    public long incrementValue(long timestamp, int factor) throws EmptySequenceCacheException {
        SequenceValue value = getSequenceValue(timestamp);
        if (value == null) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        if (value.currentValue == value.nextValue) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        long returnValue = value.currentValue;
        value.currentValue += factor * value.incrementBy;
        return returnValue;
    }

    public Append newReturn(long timestamp) throws EmptySequenceCacheException {
        SequenceValue value = getSequenceValue(timestamp);
        if (value == null) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        if (value.currentValue == value.nextValue) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }

        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        byte[] opBuf = new byte[] {(byte)SequenceRegionObserver.Op.RESET_SEQUENCE.ordinal()};
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, opBuf);
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        append.setAttribute(SequenceRegionObserver.CURRENT_VALUE_ATTRIB, PDataType.LONG.toBytes(value.nextValue));
        Map<byte[], List<KeyValue>> familyMap = append.getFamilyMap();
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<KeyValue>asList(
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, value.timestamp, PDataType.LONG.toBytes(value.currentValue))
                ));
        return append;
    }
    
    public long currentValue(long timestamp) throws EmptySequenceCacheException {
        SequenceValue value = getSequenceValue(timestamp);
        if (value == null) {
            throw EMPTY_SEQUENCE_CACHE_EXCEPTION;
        }
        return value.currentValue;
    }

    public ReentrantLock getLock() {
        return lock;
    }

    public SequenceKey getKey() {
        return key;
    }

    public long incrementValue(Result result, int factor) throws SQLException {
        // In this case, we don't definitely know the timestamp of the deleted sequence,
        // but we know anything older is likely deleted. Worse case, we remove a sequence
        // from the cache that we shouldn't have which will cause a gap in sequence values.
        // In that case, we might get an error that a curr value was done on a sequence
        // before a next val was. Not sure how to prevent that.
        if (result.raw().length == 1) {
            KeyValue errorKV = result.raw()[0];
            int errorCode = PDataType.INTEGER.getCodec().decodeInt(errorKV.getBuffer(), errorKV.getValueOffset(), null);
            SQLExceptionCode code = SQLExceptionCode.fromErrorCode(errorCode);
            if (code == SQLExceptionCode.SEQUENCE_UNDEFINED) {
                if (values != null) {
                    long timestamp = errorKV.getTimestamp();
                    // Remove any sequence values older than timestamp as we didn't find any
                    while (!values.isEmpty() && values.get(0).timestamp < timestamp) {
                        values.remove(0);
                    }
                }
            }
            throw new SQLExceptionInfo.Builder(code)
                .setSchemaName(key.getSchemaName())
                .setTableName(key.getSequenceName())
                .build().buildException();
        }
        // If we found the sequence, we update our cache with the new value
        SequenceValue sequence = new SequenceValue(result);
        if (values == null) {
            values = Lists.newArrayListWithExpectedSize(1);
            values.add(sequence);
        } else {
            int i = values.size()-1;
            while (i >= 0 && values.get(i).timestamp > sequence.timestamp) {
                i--;
            }
            if (i >= 0 && values.get(i).timestamp == sequence.timestamp) {
                values.set(i, sequence);
            } else {
                values.add(i+1, sequence);
            }
        }
        long value = sequence.currentValue;
        sequence.currentValue += factor * sequence.incrementBy;
        return value;
    }

    public Increment newIncrement(long timestamp) {
        Increment inc = new Increment(SchemaUtil.getSequenceKey(key.getTenantId(), key.getSchemaName(), key.getSequenceName()));
        // It doesn't matter what we set the amount too - we always use the values we get
        // from the Get we do to prevent any race conditions. All columns that get added
        // are returned with their current value
        try {
            inc.setTimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, timestamp);
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        for (KeyValue kv : SEQUENCE_KV_COLUMNS) {
            // We don't care about the amount, as we'll add what gets looked up on the server-side
            inc.addColumn(kv.getFamily(), kv.getQualifier(), AMOUNT);
        }
        return inc;
    }
    
    public static KeyValue getCurrentValueKV(List<KeyValue> kvs) {
        assert(kvs.size() == SEQUENCE_KEY_VALUES);
        return kvs.get(CURRENT_VALUE_INDEX);
    }
    
    public static KeyValue getIncrementByKV(List<KeyValue> kvs) {
        assert(kvs.size() == SEQUENCE_KEY_VALUES);
        return kvs.get(INCREMENT_BY_INDEX);
    }
    
    public static KeyValue getCacheSizeKV(List<KeyValue> kvs) {
        assert(kvs.size() == SEQUENCE_KEY_VALUES);
        return kvs.get(CACHE_SIZE_INDEX);
    }
    
    public static KeyValue getCurrentValueKV(Result r) {
        KeyValue[] kvs = r.raw();
        assert(kvs.length == SEQUENCE_KEY_VALUES);
        return kvs[CURRENT_VALUE_INDEX];
    }
    
    public static KeyValue getIncrementByKV(Result r) {
        KeyValue[] kvs = r.raw();
        assert(kvs.length == SEQUENCE_KEY_VALUES);
        return kvs[INCREMENT_BY_INDEX];
    }
    
    public static KeyValue getCacheSizeKV(Result r) {
        KeyValue[] kvs = r.raw();
        assert(kvs.length == SEQUENCE_KEY_VALUES);
        return kvs[CACHE_SIZE_INDEX];
    }
    
    public static Result replaceCurrentValueKV(Result r, KeyValue currentValueKV) {
        KeyValue[] kvs = r.raw();
        List<KeyValue> newkvs = Lists.newArrayList(kvs);
        newkvs.set(CURRENT_VALUE_INDEX, currentValueKV);
        return new Result(newkvs);
    }
    
    private static final class SequenceValue {
        public final long incrementBy;
        public final int cacheSize;
        public final long timestamp;
        
        public long currentValue;
        public long nextValue;
        
        public SequenceValue(long timestamp) {
            this.timestamp = timestamp;
            this.incrementBy = 0;
            this.cacheSize = 0;
        }
        
        public SequenceValue(Result r) {
            KeyValue currentValueKV = getCurrentValueKV(r);
            KeyValue incrementByKV = getIncrementByKV(r);
            KeyValue cacheSizeKV = getCacheSizeKV(r);
            timestamp = currentValueKV.getTimestamp();
            nextValue = PDataType.LONG.getCodec().decodeLong(currentValueKV.getBuffer(), currentValueKV.getValueOffset(), null);
            incrementBy = PDataType.LONG.getCodec().decodeLong(incrementByKV.getBuffer(), incrementByKV.getValueOffset(), null);
            cacheSize = PDataType.INTEGER.getCodec().decodeInt(cacheSizeKV.getBuffer(), cacheSizeKV.getValueOffset(), null);
            currentValue = nextValue - incrementBy * cacheSize;
        }
    }

    public boolean returnValue(Result result) throws SQLException {
        KeyValue statusKV = result.raw()[0];
        if (statusKV.getValueLength() == 0) { // No error, but unable to return sequence values
            return false;
        }
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getBuffer(), statusKV.getValueOffset(), null);
        if (statusCode == 0) {  // Success - update nextValue down to currentValue
            SequenceValue value = getSequenceValue(timestamp);
            if (value == null) {
                throw new EmptySequenceCacheException(key.getSchemaName(),key.getSequenceName());
            }
            value.nextValue = value.currentValue;
            return true;
        }
        SQLExceptionCode code = SQLExceptionCode.fromErrorCode(statusCode);
        if (code == SQLExceptionCode.SEQUENCE_UNDEFINED) {
            if (values != null) {
                // Remove any sequence values older than timestamp as we didn't find any
                while (!values.isEmpty() && values.get(0).timestamp < timestamp) {
                    values.remove(0);
                }
            }
        }
        throw new SQLExceptionInfo.Builder(code)
            .setSchemaName(key.getSchemaName())
            .setTableName(key.getSequenceName())
            .build().buildException();
    }

    public Append createSequence(long startWith, long incrementBy, int cacheSize, long timestamp) {
        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)SequenceRegionObserver.Op.CREATE_SEQUENCE.ordinal()});
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        Map<byte[], List<KeyValue>> familyMap = append.getFamilyMap();
        byte[] startWithBuf = PDataType.LONG.toBytes(startWith);
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<KeyValue>asList(
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CURRENT_VALUE_BYTES, timestamp, startWithBuf),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.START_WITH_BYTES, timestamp, startWithBuf),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.INCREMENT_BY_BYTES, timestamp, PDataType.LONG.toBytes(incrementBy)),
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, PhoenixDatabaseMetaData.CACHE_SIZE_BYTES, timestamp, PDataType.INTEGER.toBytes(cacheSize))
                ));
        return append;
    }

    public long createSequence(Result result) throws SQLException {
        KeyValue statusKV = result.raw()[0];
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getBuffer(), statusKV.getValueOffset(), null);
        if (statusCode == 0) {  // Success - add sequence value and return timestamp
            SequenceValue value = new SequenceValue(timestamp);
            if (values == null) {
                values = Lists.newArrayListWithExpectedSize(1);
                values.add(value);
                return timestamp;
            }
            int i = values.size()-1;
            while (i >= 0 && values.get(i).timestamp > timestamp) {
                i--;
            }
            values.add(i+1, value);
            return timestamp;
        }
        SQLExceptionCode code = SQLExceptionCode.fromErrorCode(statusCode);
        throw new SQLExceptionInfo.Builder(code)
            .setSchemaName(key.getSchemaName())
            .setTableName(key.getSequenceName())
            .build().buildException();
    }

    public Append dropSequence(long timestamp) {
        byte[] key = SchemaUtil.getSequenceKey(this.key.getTenantId(), this.key.getSchemaName(), this.key.getSequenceName());
        Append append = new Append(key);
        append.setAttribute(SequenceRegionObserver.OPERATION_ATTRIB, new byte[] {(byte)SequenceRegionObserver.Op.DROP_SEQUENCE.ordinal()});
        if (timestamp != HConstants.LATEST_TIMESTAMP) {
            append.setAttribute(SequenceRegionObserver.MAX_TIMERANGE_ATTRIB, Bytes.toBytes(timestamp));
        }
        Map<byte[], List<KeyValue>> familyMap = append.getFamilyMap();
        familyMap.put(PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, Arrays.<KeyValue>asList(
                KeyValueUtil.newKeyValue(key, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, timestamp, ByteUtil.EMPTY_BYTE_ARRAY)));
        return append;
    }

    public long dropSequence(Result result) throws SQLException {
        KeyValue statusKV = result.raw()[0];
        long timestamp = statusKV.getTimestamp();
        int statusCode = PDataType.INTEGER.getCodec().decodeInt(statusKV.getBuffer(), statusKV.getValueOffset(), null);
        SQLExceptionCode code = statusCode == 0 ? null : SQLExceptionCode.fromErrorCode(statusCode);
        if (code == null || code == SQLExceptionCode.SEQUENCE_UNDEFINED) {
            if (values != null) {
                // Remove any sequence values older than timestamp as we didn't find any
                while (!values.isEmpty() && values.get(0).timestamp < timestamp) {
                    values.remove(0);
                }
            }
        }
        if (code == null) {
            return timestamp;
        }
        throw new SQLExceptionInfo.Builder(code)
            .setSchemaName(key.getSchemaName())
            .setTableName(key.getSequenceName())
            .build().buildException();
    }
}
