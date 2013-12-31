package com.salesforce.phoenix.coprocessor;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue;
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

import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.KeyValueUtil;
import com.salesforce.phoenix.util.ServerUtil;

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
public class PreIncrementRegionObserver extends BaseRegionObserver {
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
                Put put = new Put(row);
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

}
