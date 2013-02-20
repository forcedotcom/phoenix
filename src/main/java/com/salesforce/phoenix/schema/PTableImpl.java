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
package com.salesforce.phoenix.schema;

import static com.salesforce.phoenix.query.QueryConstants.SEPARATOR_BYTE;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.*;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.util.*;



/**
 * 
 * Base class for PTable implementors.  Provides abstraction for
 * storing data in a single column (ColumnLayout.SINGLE) or in
 * multiple columns (ColumnLayout.MULTI).
 *
 * @author jtaylor
 * @since 0.1
 */
public class PTableImpl implements PTable {
    private PName name;
    private PTableType type;
    private long sequenceNumber;
    private long timeStamp;
    // Have MultiMap for String->PColumn (may need family qualifier)
    private List<PColumn> pkColumns;
    private List<PColumn> allColumns;
    private List<PColumnFamily> families;
    private Map<byte[], PColumnFamily> familyByBytes;
    private Map<String, PColumnFamily> familyByString;
    private ListMultimap<String,PColumn> columnsByName;
    private String pkName;
    
    public PTableImpl() {
    }
    
    public PTableImpl(long timeStamp) { // For delete marker
        this.type = PTableType.USER;
        this.timeStamp = timeStamp;
        this.pkColumns = this.allColumns = Collections.emptyList();
        this.families = Collections.emptyList();
        this.familyByBytes = Collections.emptyMap();
        this.familyByString = Collections.emptyMap();
    }

    public PTableImpl(PName name, PTableType type, long timeStamp, long sequenceNumber, String pkName, List<PColumn> columns) {
        init(name, type, timeStamp, sequenceNumber, pkName, columns);
    }
    
    @Override
    public String toString() {
        return name.getString();
    }
    
    private void init(PName name, PTableType type, long timeStamp, long sequenceNumber, String pkName, List<PColumn> columns) {
        this.name = name;
        this.type = type;
        this.timeStamp = timeStamp;
        this.sequenceNumber = sequenceNumber;
        this.columnsByName = ArrayListMultimap.create(columns.size(), 1);
        this.pkName = pkName;
        List<PColumn> pkColumns = Lists.newArrayListWithExpectedSize(columns.size()-1);
        PColumn[] allColumns = new PColumn[columns.size()];
        for (int i = 0; i < allColumns.length; i++) {
            PColumn column = columns.get(i);
            allColumns[column.getPosition()] = column;
            PName familyName = column.getFamilyName();
            if (familyName == null) {
                pkColumns.add(column);
            }
            columnsByName.put(column.getName().getString(), column);
        }
        this.pkColumns = ImmutableList.copyOf(pkColumns);
        this.allColumns = ImmutableList.copyOf(allColumns);
        
        // Two pass so that column order in column families matches overall column order
        // and to ensure that column family order is constant
        int maxExpectedSize = allColumns.length - pkColumns.size();
        // Maintain iteration order so that column families are ordered as they are listed
        Map<PName, List<PColumn>> familyMap = Maps.newLinkedHashMap();
        for (PColumn column : allColumns) {
            PName familyName = column.getFamilyName();
            if (familyName != null) {
                List<PColumn> columnsInFamily = familyMap.get(familyName);
                if (columnsInFamily == null) {
                    columnsInFamily = Lists.newArrayListWithExpectedSize(maxExpectedSize);
                    familyMap.put(familyName, columnsInFamily);
                }
                columnsInFamily.add(column);
            }
        }
        
        Iterator<Map.Entry<PName,List<PColumn>>> iterator = familyMap.entrySet().iterator();
        PColumnFamily[] families = new PColumnFamily[familyMap.size()];
        ImmutableMap.Builder<String, PColumnFamily> familyByString = ImmutableMap.builder();
        ImmutableSortedMap.Builder<byte[], PColumnFamily> familyByBytes = ImmutableSortedMap.orderedBy(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < families.length; i++) {
            Map.Entry<PName,List<PColumn>> entry = iterator.next();
            PColumnFamily family = new PColumnFamilyImpl(entry.getKey(), entry.getValue());
            families[i] = family;
            familyByString.put(family.getName().getString(), family);
            familyByBytes.put(family.getName().getBytes(), family);
        }
        this.families = ImmutableList.copyOf(families);
        this.familyByBytes = familyByBytes.build();
        this.familyByString = familyByString.build();
    }
    
    @Override
    public List<PColumn> getPKColumns() {
        return pkColumns;
    }

    @Override
    public final PName getName() {
        return name;
    }
    
    @Override
    public final PTableType getType() {
        return type;
    }

    @Override
    public final List<PColumnFamily> getColumnFamilies() {
        return families;
    }
    
    @Override
    public int newKey(ImmutableBytesWritable key, byte[][] values) {
        int i = 0;
        TrustedByteArrayOutputStream os = new TrustedByteArrayOutputStream(SchemaUtil.estimateKeyLength(this));
        List<PColumn> columns = getPKColumns();
        int nColumns = columns.size();
        PColumn lastPKColumn = columns.get(nColumns - 1);
        while (i < values.length && i < nColumns) {
            PColumn column = columns.get(i);
            PDataType type = column.getDataType();
            // This will throw if the value is null and the type doesn't allow null
            byte[] byteValue = values[i++];
            if (byteValue == null) {
                byteValue = ByteUtil.EMPTY_BYTE_ARRAY;
            }
            // An empty byte array return value means null. Do this,
            // since a type may have muliple representations of null.
            // For example, VARCHAR treats both null and an empty string
            // as null. This way we don't need to leak that part of the
            // implementation outside of PDataType by checking the value
            // here.
            if (byteValue.length == 0 && !column.isNullable()) { 
                throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not be null");
            }
            Integer maxLength = column.getMaxLength();
            if (type.isFixedWidth()) { // TODO: handle multi-byte characters
                if (byteValue.length != maxLength) {
                    throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " must be " + maxLength + " bytes (" + SchemaUtil.toString(type, byteValue) + ")");
                }
            } else if (maxLength != null && byteValue.length > maxLength) {
                throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not exceed " + maxLength + " bytes (" + SchemaUtil.toString(type, byteValue) + ")");
            }
            os.write(byteValue, 0, byteValue.length);
            // Separate variable length column values in key with zero byte
            if (!type.isFixedWidth() && column != lastPKColumn) {
                os.write(SEPARATOR_BYTE);
            }
        }
        // If some non null pk values aren't set, then throw
        if (i < nColumns) {
            PColumn column = columns.get(i);
            PDataType type = column.getDataType();
            if (type.isFixedWidth() || !column.isNullable()) {
                throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not be null");
            }
            // Separate variable length column values in key with zero byte
            if (column != lastPKColumn) {
                os.write(SEPARATOR_BYTE);
            }
        }
        key.set(os.getBuffer(),0,os.size());
        return i;
    }

    private PRow newRow(long ts, ImmutableBytesWritable key, int i, Object[] values) {
        PRow row = new PRowImpl(key, ts);
        if (i < values.length) {
            for (PColumnFamily family : getColumnFamilies()) {
                for (PColumn column : family.getColumns()) {
                    row.setValue(column, values[i++]);
                    if (i == values.length)
                        return row;
                }
            }
        }
        return row;
    }

    @Override
    public PRow newRow(long ts, ImmutableBytesWritable key, byte[]... values) {
        return newRow(ts, key, 0, values);
    }

    @Override
    public PRow newRow(ImmutableBytesWritable key, byte[]... values) {
        return newRow(HConstants.LATEST_TIMESTAMP, key, values);
    }

    @Override
    public PColumn getColumn(String name) throws ColumnNotFoundException, AmbiguousColumnException {
        List<PColumn> columns = columnsByName.get(name);
        int size = columns.size();
        if (size == 0) {
            throw new ColumnNotFoundException(name);
        }
        if (size > 1) {
            for (PColumn column : columns) {
                if (QueryConstants.DEFAULT_COLUMN_FAMILY.equals(column.getFamilyName().getString())) {
                    // Allow ambiguity with default column, since a user would not know how to prefix it.
                    return column;
                }
            }
            throw new AmbiguousColumnException(name);
        }
        return columns.get(0);
    }
    
    /**
     * 
     * PRow implementation for ColumnLayout.MULTI mode which stores column
     * values across multiple hbase columns.
     *
     * @author jtaylor
     * @since 0.1
     */
    private class PRowImpl implements PRow {
        private final byte[] key;
        private Put setValues;
        private Delete unsetValues;
        private Delete deleteRow;
        private final long ts;
        
        public PRowImpl(ImmutableBytesWritable key, long ts) {
            byte[] keyBytes = key.copyBytes();
            this.setValues = new Put(keyBytes);
            this.unsetValues = new Delete(keyBytes);
            this.ts = ts;
            this.key = keyBytes;
        }
        
        @Override
        public List<Mutation> toRowMutations() { // TODO: change to List<Mutation> once it implements Row
            List<Mutation> mutations = new ArrayList<Mutation>(3);
            if (deleteRow != null) {
                // Include only deleteRow mutation if present because it takes precedence over all others
                mutations.add(deleteRow);
            } else {
                // Because we cannot enforce a not null constraint on a KV column (since we don't know if the row exists when
                // we upsert it), se instead add a KV that is always emtpy. This allows us to imitate SQL semantics given the
                // way HBase works.
                setValues.add(SchemaUtil.getEmptyColumnFamily(getColumnFamilies()), QueryConstants.EMPTY_COLUMN_BYTES, ts, ByteUtil.EMPTY_BYTE_ARRAY);
                mutations.add(setValues);
                if (!unsetValues.isEmpty()) {
                    mutations.add(unsetValues);
                }
            }
            return mutations;
        }

        private void removeIfPresent(Mutation m, byte[] family, byte[] qualifier) {
            Map<byte[],List<KeyValue>> familyMap = m.getFamilyMap();
            List<KeyValue> kvs = familyMap.get(family);
            if (kvs != null) {
                Iterator<KeyValue> iterator = kvs.iterator();
                while (iterator.hasNext()) {
                    KeyValue kv = iterator.next();
                    if (Bytes.compareTo(kv.getQualifier(), qualifier) == 0) {
                        iterator.remove();
                    }
                }
            }
        }
        
        @Override
        public void setValue(PColumn column, Object value) {
            byte[] byteValue = value == null ? ByteUtil.EMPTY_BYTE_ARRAY : column.getDataType().toBytes(value);
            setValue(column, byteValue);
        }
        
        @Override
        public void setValue(PColumn column, byte[] byteValue) {
            deleteRow = null;
            byte[] family = column.getFamilyName().getBytes();
            byte[] qualifier = column.getName().getBytes();
            PDataType type = column.getDataType();
            // Check null, since some types have no byte representation for null
            if (byteValue == null || byteValue.length == 0) {
                if (!column.isNullable()) { 
                    throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not be null");
                }
                removeIfPresent(setValues, family, qualifier);
                unsetValues.deleteColumns(family, qualifier, ts);
            } else {
                Integer maxLength = column.getMaxLength();
                if (type.isFixedWidth()) { // TODO: handle multi-byte characters
                    if (byteValue.length != maxLength) {
                        throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " must be " + maxLength + " bytes (" + type.toObject(byteValue) + ")");
                    }
                } else if (maxLength != null && byteValue.length > maxLength) {
                    throw new ConstraintViolationException(name.getString() + "." + column.getName().getString() + " may not exceed " + maxLength + " bytes (" + type.toObject(byteValue) + ")");
                }
                removeIfPresent(unsetValues, family, qualifier);
                setValues.add(family, qualifier, ts, byteValue);
            }
        }

        @Override
        public void delete() {
            setValues = new Put(key);
            unsetValues = new Delete(key);
            @SuppressWarnings("deprecation") // FIXME: Remove when unintentionally deprecated method is fixed (HBASE-7870).
            Delete delete = new Delete(key,ts);
            deleteRow = delete;
        }
    }

    @Override
    public PColumnFamily getColumnFamily(String familyName) throws ColumnFamilyNotFoundException {
        PColumnFamily family = familyByString.get(familyName);
        if (family == null) {
            throw new ColumnFamilyNotFoundException(familyName);
        }
        return family;
    }
    
    @Override
    public PColumnFamily getColumnFamily(byte[] familyBytes) throws ColumnFamilyNotFoundException {
        PColumnFamily family = familyByBytes.get(familyBytes);
        if (family == null) {
            String familyName = Bytes.toString(familyBytes);
            throw new ColumnFamilyNotFoundException(familyName);
        }
        return family;
    }

    @Override
    public List<PColumn> getColumns() {
        return allColumns;
    }

    @Override
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public long getTimeStamp() {
        return timeStamp;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        byte[] tableNameBytes = Bytes.readByteArray(input);
        PName tableName = new PNameImpl(tableNameBytes);
        PTableType tableType = PTableType.values()[WritableUtils.readVInt(input)];
        long sequenceNumber = WritableUtils.readVLong(input);
        long timeStamp = input.readLong();
        byte[] pkNameBytes = Bytes.readByteArray(input);
        String pkName = pkNameBytes.length == 0 ? null : Bytes.toString(pkNameBytes);
        int nColumns = WritableUtils.readVInt(input);
        List<PColumn> columns = Lists.newArrayListWithExpectedSize(nColumns);
        for (int i = 0; i < nColumns; i++) {
            PColumn column = new PColumnImpl();
            column.readFields(input);
            columns.add(column);
        }
        init(tableName, tableType, timeStamp, sequenceNumber, pkName, columns);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        Bytes.writeByteArray(output, name.getBytes());
        WritableUtils.writeVInt(output, type.ordinal());
        WritableUtils.writeVLong(output, sequenceNumber);
        output.writeLong(timeStamp);
        Bytes.writeByteArray(output, pkName == null ? ByteUtil.EMPTY_BYTE_ARRAY : Bytes.toBytes(pkName));
        WritableUtils.writeVInt(output, allColumns.size());
        for (int i = 0; i < allColumns.size(); i++) {
            PColumn column = allColumns.get(i);
            column.write(output);
        }
    }

    @Override
    public PColumn getPKColumn(String name) throws ColumnNotFoundException {
        List<PColumn> columns = columnsByName.get(name);
        int size = columns.size();
        if (size == 0) {
            throw new ColumnNotFoundException(name);
        }
        if (size > 1) {
            do {
                PColumn column = columns.get(--size);
                if (column.getFamilyName() == null) {
                    return column;
                }
            } while (size > 0);
            throw new ColumnNotFoundException(name);
        }
        return columns.get(0);
    }

    @Override
    public String getPKName() {
        return pkName;
    }
}
