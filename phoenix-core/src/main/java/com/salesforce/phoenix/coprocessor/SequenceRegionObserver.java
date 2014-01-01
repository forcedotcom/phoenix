package com.salesforce.phoenix.coprocessor;

import java.io.IOException;
import java.util.Collections;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.MetaDataUtil;
import com.salesforce.phoenix.util.ResultUtil;
import com.salesforce.phoenix.util.ServerUtil;

/**
 * 
 * Region observer coprocessor for sequence operations:
 * 1) For creating a sequence, as checkAndPut does not allow us to scope the
 * Get done for the check with a TimeRange.
 * 2) For incrementing a sequence, as increment does not a) allow us to set the
 * timestamp of the key value being incremented and b) recognize when the key
 * value being incremented does not exist
 * 3) For deleting a sequence, as checkAndDelete does not allow us to scope
 * the Get done for the check with a TimeRange.
 *
 * @author jtaylor
 * @since 3.0.0
 */
public class SequenceRegionObserver extends BaseRegionObserver {
    public enum SequenceOp {CREATE, DROP, RESET};
    public static final String OPERATION_ATTRIB = "SEQUENCE_OPERATION";
    public static final String MAX_TIMERANGE_ATTRIB = "MAX_TIMERANGE";
    public static final String CURRENT_VALUE_ATTRIB = "CURRENT_VALUE";
    /**
     * 
     * Use PreIncrement hook of BaseRegionObserver to overcome deficiencies in Increment
     * implementation (HBASE-10254):
     * 1) Lack of recognition and identification of when the key value to increment doesn't exist
     * 2) Lack of the ability to set the timestamp of the updated key value.
     * Works the same as existing region.increment(), except assumes there is a single column to
     * increment and uses Phoenix LONG encoding.
     * @author jtaylor
     * @since 3.0.0
     */
    @Override
    public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment) throws IOException {
        RegionCoprocessorEnvironment env = e.getEnvironment();
        // We need to set this to prevent region.increment from being called
        e.bypass();
        e.complete();
        HRegion region = env.getRegion();
        byte[] row = increment.getRow();
        TimeRange tr = increment.getTimeRange();
        region.startRegionOperation();
        try {
            Integer lid = region.getLock(null, row, true);
            try {
                NavigableMap<byte[], Long> keyValueMap = increment.getFamilyMap().values().iterator().next();
                byte[] family = increment.getFamilyMap().keySet().iterator().next();
                byte[] qualifier = keyValueMap.firstKey();
                long incrementBy = keyValueMap.values().iterator().next();

                Get get = new Get(row);
                get.setTimeRange(tr.getMin(), tr.getMax());
                get.addColumn(family, qualifier);
                Result result = region.get(get);
                if (result.isEmpty()) {
                    return result;
                }
                KeyValue currentValueKV = result.raw()[0];
                long value = PDataType.LONG.getCodec().decodeLong(currentValueKV.getBuffer(), currentValueKV.getValueOffset(), null);
                value += incrementBy;
                byte[] valueBuffer = new byte[PDataType.LONG.getByteSize()];
                PDataType.LONG.getCodec().encodeLong(value, valueBuffer, 0);
                Put put = new Put(row, currentValueKV.getTimestamp());
                // Hold timestamp constant for sequences, so that clients always only see the latest value
                // regardless of when they connect.
                KeyValue newKV = KeyValueUtil.newKeyValue(row, family, qualifier, currentValueKV.getTimestamp(), valueBuffer);
                put.add(newKV);
                @SuppressWarnings("unchecked")
                Pair<Mutation,Integer>[] mutations = new Pair[1];
                mutations[0] = new Pair<Mutation,Integer>(put, lid);
                region.batchMutate(mutations);
                return new Result(put.getFamilyMap().values().iterator().next());
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        } finally {
            region.closeRegionOperation();
        }
    }

    /**
     * Override the preAppend for checkAndPut and checkAndDelete, as we need the ability to
     * a) set the TimeRange for the Get being done and
     * b) return something back to the client to indicate success/failure
     */
    @SuppressWarnings("deprecation")
    @Override
    public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
            final Append append) throws IOException {
        byte[] opBuf = append.getAttribute(OPERATION_ATTRIB);
        if (opBuf == null) {
            return null;
        }
        SequenceOp op = SequenceOp.values()[opBuf[0]];

        long clientTimestamp = HConstants.LATEST_TIMESTAMP;
        byte[] maxTimeRangeBuf = append.getAttribute(MAX_TIMERANGE_ATTRIB);
        if (maxTimeRangeBuf != null) {
            clientTimestamp = PDataType.LONG.getCodec().decodeLong(maxTimeRangeBuf, 0, null);
        }

        RegionCoprocessorEnvironment env = e.getEnvironment();
        // We need to set this to prevent region.append from being called
        e.bypass();
        e.complete();
        HRegion region = env.getRegion();
        byte[] row = append.getRow();
        TimeRange tr = new TimeRange(MetaDataProtocol.MIN_TABLE_TIMESTAMP, clientTimestamp);
        region.startRegionOperation();
        try {
            Integer lid = region.getLock(null, row, true);
            try {
                KeyValue keyValue = append.getFamilyMap().values().iterator().next().iterator().next();
                byte[] family = keyValue.getFamily();
                byte[] qualifier = keyValue.getQualifier();

                Get get = new Get(row);
                get.setTimeRange(tr.getMin(), tr.getMax());
                get.addColumn(family, qualifier);
                Result result = region.get(get);
                if (result.isEmpty()) {
                    if (op == SequenceOp.DROP || op == SequenceOp.RESET) {
                        return ResultUtil.EMPTY_RESULT;
                    }
                } else {
                    if (op == SequenceOp.CREATE) {
                        return ResultUtil.EMPTY_RESULT;
                    }
                }
                Mutation m = null;
                switch (op) {
                case RESET:
                    KeyValue currentValueKV = result.raw()[0];
                    long expectedValue = PDataType.LONG.getCodec().decodeLong(append.getAttribute(CURRENT_VALUE_ATTRIB), 0, null);
                    long value = PDataType.LONG.getCodec().decodeLong(currentValueKV.getBuffer(), currentValueKV.getValueOffset(), null);
                    if (expectedValue != value) {
                        return new Result(Collections.singletonList(KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, currentValueKV.getTimestamp(), PDataType.FALSE_BYTES)));
                    }
                    m = new Put(row, currentValueKV.getTimestamp());
                    m.getFamilyMap().putAll(append.getFamilyMap());
                    break;
                case DROP:
                    m = new Delete(row, clientTimestamp, null);
                    break;
                case CREATE:
                    m = new Put(row, clientTimestamp);
                    m.getFamilyMap().putAll(append.getFamilyMap());
                    break;
                }
                @SuppressWarnings("unchecked")
                Pair<Mutation,Integer>[] mutations = new Pair[1];
                mutations[0] = new Pair<Mutation,Integer>(m, lid);
                region.batchMutate(mutations);
                long serverTimestamp = MetaDataUtil.getClientTimeStamp(m);
                // Return result with single KeyValue. The only piece of information
                // the client cares about is the timestamp, which is the timestamp of
                // when the mutation was actually performed (useful in the case of .
                return new Result(Collections.singletonList(KeyValueUtil.newKeyValue(row, PhoenixDatabaseMetaData.SEQUENCE_FAMILY_BYTES, QueryConstants.EMPTY_COLUMN_BYTES, serverTimestamp, PDataType.TRUE_BYTES)));
            } finally {
                region.releaseRowLock(lid);
            }
        } catch (Throwable t) {
            ServerUtil.throwIOException("Increment of sequence " + Bytes.toStringBinary(row), t);
            return null; // Impossible
        } finally {
            region.closeRegionOperation();
        }
    }

}
