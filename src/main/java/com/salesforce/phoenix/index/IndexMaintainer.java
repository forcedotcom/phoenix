package com.salesforce.phoenix.index;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.builder.covered.ColumnReference;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.AmbiguousColumnException;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.schema.ValueBitSet;
import com.salesforce.phoenix.schema.ValueSchema;
import com.salesforce.phoenix.util.BitSet;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.IndexUtil;
import com.salesforce.phoenix.util.SchemaUtil;
import com.salesforce.phoenix.util.TrustedByteArrayOutputStream;

public class IndexMaintainer implements Writable {
    private List<ColumnReference> indexedColumns;
    private List<ColumnReference> coveredColumns;
    private List<ColumnReference> allColumns;
    private List<PDataType> indexedColumnTypes;
    private List<Integer> indexedColumnByteSizes;
    private RowKeySchema rowKeySchema;
    private RowKeyValueIterator iterator;
    private byte[] indexTableName;
    
    public IndexMaintainer() {
    }

    public IndexMaintainer(byte[] indexTableName, List<ColumnReference> indexedColumns, List<PDataType> indexedColumnTypes, List<Integer> indexedColumnByteSizes, List<ColumnReference> coveredColumns, RowKeySchema rowKeySchema, 
            BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet, BitSet pkNotNullableBitSet) {
        this.indexTableName = indexTableName;
        this.indexedColumns = indexedColumns;
        this.indexedColumnTypes = indexedColumnTypes;
        this.indexedColumnByteSizes = indexedColumnByteSizes;
        this.coveredColumns = coveredColumns;
        this.allColumns = Lists.newArrayListWithExpectedSize(indexedColumns.size() + coveredColumns.size());
        allColumns.addAll(indexedColumns);
        allColumns.addAll(coveredColumns);
        this.rowKeySchema = rowKeySchema;
        iterator = newIterator(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet, pkNotNullableBitSet);
    }
    
    public List<ColumnReference> getIndexedColumns() {
        return indexedColumns;
    }

    public List<ColumnReference> getAllColumns() {
        return allColumns;
    }
    
    public RowKeyValueIterator iterator() {
        return iterator;
    }

    public static List<IndexMaintainer> deserialize(byte[] mdValue) {
        ByteArrayInputStream stream = new ByteArrayInputStream(mdValue);
        DataInput input = new DataInputStream(stream);
        List<IndexMaintainer> maintainers = Collections.emptyList();
        try {
            int size = WritableUtils.readVInt(input);
            maintainers = Lists.newArrayListWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                IndexMaintainer maintainer = new IndexMaintainer();
                maintainer.readFields(input);
                maintainers.add(maintainer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        return maintainers;
    }
    /**
     * For client-side to serialize all IndexMaintainers for a given table
     * @param dataTable
     * @throws IOException 
     */
    public static void serialize(byte[] schemaName, PTable dataTable, ImmutableBytesWritable ptr) {
        int nDataTablePKColumns = dataTable.getPKColumns().size();
        List<PTable> indexes = dataTable.getIndexes();
        if (indexes.isEmpty()) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return;
        }
        IndexMaintainer[] maintainers = new IndexMaintainer[indexes.size()];
        for (int i = 0; i < maintainers.length; i++) {
            PTable index = indexes.get(i);
            int totalIndexedColumns = index.getPKColumns().size();
            BitSet pkColumnBitSet = BitSet.withCapacity(totalIndexedColumns);
            BitSet descDataColumnBitSet = BitSet.withCapacity(totalIndexedColumns);
            BitSet descIndexColumnBitSet = BitSet.withCapacity(totalIndexedColumns);
            BitSet pkNotNullableBitSet = BitSet.withCapacity(totalIndexedColumns);
            List<ColumnReference>indexedColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(totalIndexedColumns-nDataTablePKColumns);
            List<PDataType>indexedColumnTypes = Lists.<PDataType>newArrayListWithExpectedSize(totalIndexedColumns-nDataTablePKColumns);
            List<Integer>indexedColumnSizes = Lists.<Integer>newArrayListWithExpectedSize(totalIndexedColumns-nDataTablePKColumns);
            List<ColumnReference>coveredColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(index.getColumns().size()-totalIndexedColumns);
            maintainers[i] = new IndexMaintainer(SchemaUtil.getTableName(schemaName, index.getName().getBytes()),
                    indexedColumns, indexedColumnTypes, indexedColumnSizes, coveredColumns, dataTable.getRowKeySchema(), 
                    pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet, pkNotNullableBitSet);
        }
        /*
         * Make one pass through all data columns. We need to lead with this, because
         * we have no unambiguous way currently of going from an index column back to
         * the data column. TODO: review, as this would be useful
         */
        int pkPosition = 0;
        // TODO: handle salted column in data and/or index table
        for (PColumn column : dataTable.getColumns()) {
            String name = IndexUtil.getIndexColumnName(column);
            boolean isPKColumn = SchemaUtil.isPKColumn(column);
            for (int i = 0; i < maintainers.length; i++) {
                IndexMaintainer maintainer = maintainers[i];
                RowKeyValueIterator iterator = maintainer.iterator();
                try {
                    PColumn indexColumn = indexes.get(i).getColumn(name);
                    if (isPKColumn) {
                        iterator.setPosition(indexColumn.getPosition(), pkPosition);
                        iterator.getPkColumnBitSet().set(indexColumn.getPosition());
                        if (!column.isNullable()) {
                            iterator.getPkNotNullableBitSet().set(indexColumn.getPosition());
                        }
                        if (column.getColumnModifier() != null) {
                            iterator.getDescDataColumnBitSet().set(indexColumn.getPosition());
                        }
                    } else {
                        if (SchemaUtil.isPKColumn(indexColumn)) {
                            iterator.setPosition(indexColumn.getPosition(), maintainer.getIndexedColumns().size());
                            maintainer.getIndexedColumnTypes().add(column.getDataType());
                            maintainer.getIndexedColumnSizes().add(column.getByteSize());
                            maintainer.getIndexedColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
                        } else {
                            maintainer.getCoverededColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
                        }
                    }
                    if (indexColumn.getColumnModifier() != null) {
                        iterator.getDescIndexColumnBitSet().set(indexColumn.getPosition());
                    }
                } catch (ColumnNotFoundException e) {
                    
                } catch (AmbiguousColumnException e) {
                    throw new IllegalStateException(e);
                }
            }
            if (isPKColumn) {
                pkPosition++;
            }
        }
        int estimatedSize = 0;
        for (int i = 0; i < maintainers.length; i++) {
            estimatedSize += maintainers[i].getEstimatedByteSize();
        }
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedSize + 1);
        DataOutput output = new DataOutputStream(stream);
        try {
            WritableUtils.writeVInt(output, maintainers.length);
            for (int i = 0; i < maintainers.length; i++) {
                maintainers[i].write(output);
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        ptr.set(stream.getBuffer(), 0, stream.size());
    }
    
    private List<Integer> getIndexedColumnSizes() {
        return indexedColumnByteSizes;
    }

    private List<PDataType> getIndexedColumnTypes() {
        return indexedColumnTypes;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        indexTableName = Bytes.readByteArray(input);
        int nIndexedColumns = WritableUtils.readVInt(input);
        indexedColumns = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            byte[] cf = Bytes.readByteArray(input);
            byte[] cq = Bytes.readByteArray(input);
            indexedColumns.add(new ColumnReference(cq,cf));
        }
        indexedColumnTypes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            PDataType type = PDataType.values()[WritableUtils.readVInt(input)];
            indexedColumnTypes.add(type);
        }
        indexedColumnByteSizes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            int byteSize = WritableUtils.readVInt(input);
            Integer byteSizeInt = byteSize <= 0 ? null : byteSize;
            indexedColumnByteSizes.add(byteSizeInt);
        }
        int nCoveredColumns = WritableUtils.readVInt(input);
        coveredColumns = Lists.newArrayListWithExpectedSize(nCoveredColumns);
        for (int i = 0; i < nCoveredColumns; i++) {
            byte[] cf = Bytes.readByteArray(input);
            byte[] cq = Bytes.readByteArray(input);
            coveredColumns.add(new ColumnReference(cq,cf));
        }
        this.allColumns = Lists.newArrayListWithExpectedSize(indexedColumns.size() + coveredColumns.size());
        allColumns.addAll(indexedColumns);
        allColumns.addAll(coveredColumns);
        rowKeySchema = new RowKeySchema();
        rowKeySchema.readFields(input);
        int totalIndexedColumns = rowKeySchema.getFieldCount() + nIndexedColumns;
        BitSet pkColumnBitSet = BitSet.read(input, totalIndexedColumns);
        BitSet descDataColumnBitSet = BitSet.read(input, totalIndexedColumns);
        BitSet descIndexColumnBitSet = BitSet.read(input, totalIndexedColumns);
        BitSet pkNotNullableBitSet = BitSet.read(input, totalIndexedColumns);
        iterator = newIterator(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet, pkNotNullableBitSet);
        iterator.readFields(input);
    }

    private RowKeyValueIterator newIterator(BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet, BitSet pkNotNullableBitSet) {
        int capacity = rowKeySchema.getFieldCount() + indexedColumns.size();
        return capacity <= 0xFF ? 
                new ByteRowKeyValueIterator(new byte[capacity], pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet, pkNotNullableBitSet) : 
                new IntRowKeyValueIterator(new int[capacity], pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet, pkNotNullableBitSet);
    }

    public int getEstimatedByteSize() {
        int size = 0;
        size += WritableUtils.getVIntSize(indexedColumns.size());
        for (int i = 0; i < indexedColumns.size(); i++) {
            ColumnReference ref = indexedColumns.get(i);
            size += WritableUtils.getVIntSize(ref.getFamily().length);
            size += ref.getFamily().length;
            size += WritableUtils.getVIntSize(ref.getQualifier().length);
            size += ref.getQualifier().length;
        }
        size += indexedColumnTypes.size();
        size += indexedColumnByteSizes.size();
        for (int i = 0; i < coveredColumns.size(); i++) {
            ColumnReference ref = coveredColumns.get(i);
            size += WritableUtils.getVIntSize(ref.getFamily().length);
            size += ref.getFamily().length;
            size += WritableUtils.getVIntSize(ref.getQualifier().length);
            size += ref.getQualifier().length;
        }
        size += rowKeySchema.getEstimatedByteSize();
        size += iterator.getByteSize();
        return size;
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        Bytes.writeByteArray(output, indexTableName);
        WritableUtils.writeVInt(output, indexedColumns.size());
        for (int i = 0; i < indexedColumns.size(); i++) {
            ColumnReference ref = indexedColumns.get(i);
            Bytes.writeByteArray(output, ref.getFamily());
            Bytes.writeByteArray(output, ref.getQualifier());
        }
        for (int i = 0; i < indexedColumnTypes.size(); i++) {
            PDataType type = indexedColumnTypes.get(i);
            WritableUtils.writeVInt(output, type.ordinal());
        }
        for (int i = 0; i < indexedColumnByteSizes.size(); i++) {
            Integer byteSize = indexedColumnByteSizes.get(i);
            WritableUtils.writeVInt(output, byteSize == null ? 0 : byteSize);
        }
        WritableUtils.writeVInt(output, coveredColumns.size());
        for (int i = 0; i < coveredColumns.size(); i++) {
            ColumnReference ref = coveredColumns.get(i);
            Bytes.writeByteArray(output, ref.getFamily());
            Bytes.writeByteArray(output, ref.getQualifier());
        }
        rowKeySchema.write(output);
        iterator.write(output);
    }

    private static void writeInverted(byte[] buf, int offset, int length, DataOutput output) throws IOException {
        for (int i = offset; i < offset + length; i++) {
            byte b = ColumnModifier.SORT_DESC.apply(buf[i]);
            output.write(b);
        }
    }
    
    public abstract class RowKeyValueIterator implements Writable {
        private final BitSet pkColumnBitSet;
        private final BitSet descDataColumnBitSet;
        private final BitSet descIndexColumnBitSet;
        private final BitSet pkNotNullableBitSet;
        private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        private final ImmutableBytesWritable rowKeyPtr = new ImmutableBytesWritable();
        
        private int index;
        private Map<ColumnReference, byte[]> valueMap;
        
        RowKeyValueIterator(BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet, BitSet pkNotNullableBitSet) {
            this.pkColumnBitSet = pkColumnBitSet;
            this.descDataColumnBitSet = descDataColumnBitSet;
            this.descIndexColumnBitSet = descIndexColumnBitSet;
            this.pkNotNullableBitSet = pkNotNullableBitSet;
        }
        
        protected int getByteSize() {
            return BitSet.getByteSize(getLength()) * 3;
        }
        
        protected abstract int getLength();
        protected abstract int getPosition(int index);
        protected abstract int setPosition(int index, int position);
        
        @Override
        public void readFields(DataInput input) throws IOException {
            
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            int length = getLength();
            BitSet.write(output, pkColumnBitSet, length);
            BitSet.write(output, descDataColumnBitSet, length);
            BitSet.write(output, descIndexColumnBitSet, length);
            BitSet.write(output, pkNotNullableBitSet, length);
        }

        private boolean writeNextKeyPart(DataOutput output) throws IOException {
            index++;
            int position = getPosition(index);
            ColumnModifier dataColumnModifier = null;
            PDataType dataColumnType;
            PDataType indexColumnType;
            if (pkColumnBitSet.get(index)) {
                dataColumnType = rowKeySchema.getField(position).getType();
                indexColumnType = IndexUtil.getIndexColumnDataType(!pkNotNullableBitSet.get(position), dataColumnType);
                int nSkipFields = 0;
                if (index > 0 && pkColumnBitSet.get(getPosition(index-1)) && (nSkipFields=position - getPosition(index-1)) > 0 ) { // Increasing, then start from ptr
                    for (int i = position-nSkipFields+1; i <= position; i++) {
                        if (rowKeySchema.next(ptr, i, rowKeyPtr.getOffset()+rowKeyPtr.getLength(), ValueBitSet.EMPTY_VALUE_BITSET) == null) {
                            return indexColumnType.isFixedWidth();
                        }
                    }
                } else { // Reposition from beginning
                    this.ptr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
                    int maxOffset = ptr.getOffset() + ptr.getLength();
                    int i = 0;
                    Boolean  hasValue;
                    for (hasValue = rowKeySchema.first(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET); i < position && hasValue != null; hasValue=rowKeySchema.next(ptr, ++i, maxOffset, ValueBitSet.EMPTY_VALUE_BITSET)) {
                    }
                    if (hasValue == null) {
                        return indexColumnType.isFixedWidth();
                    }
                }
                boolean isDataColumnInverted = descDataColumnBitSet.get(position);
                if (dataColumnType.isBytesComparableWith(indexColumnType) && isDataColumnInverted == descIndexColumnBitSet.get(position)) {
                    output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                    return indexColumnType.isFixedWidth();
                }
                dataColumnModifier = isDataColumnInverted ? ColumnModifier.SORT_DESC : null;
            } else {
                dataColumnType = indexedColumnTypes.get(position);
                indexColumnType = IndexUtil.getIndexColumnDataType(true, dataColumnType);
                byte[] value = valueMap.get(indexedColumns.get(position));
                if (value == null) {
                    return indexColumnType.isFixedWidth();
                }
                ptr.set(value);
            }
            if (!dataColumnType.isBytesComparableWith(indexColumnType)) {
                indexColumnType.coerceBytes(ptr, dataColumnType, dataColumnModifier, null);
            }
            if (descIndexColumnBitSet.get(position)) {
                writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
            } else {
                output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
            }
            return indexColumnType.isFixedWidth();
        }

        public void init(Map<ColumnReference, byte[]> valueMap, ImmutableBytesWritable rowKeyPtr) {
            index = -1;
            this.valueMap = valueMap;
            this.ptr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
            this.rowKeyPtr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
        }

        public BitSet getPkColumnBitSet() {
            return pkColumnBitSet;
        }

        public BitSet getDescDataColumnBitSet() {
            return descDataColumnBitSet;
        }

        public BitSet getDescIndexColumnBitSet() {
            return descIndexColumnBitSet;
        }
        
        public BitSet getPkNotNullableBitSet() {
            return pkNotNullableBitSet;
        }
    }
    
    private class ByteRowKeyValueIterator extends RowKeyValueIterator {
        private final byte[] columnPosition;
        
        ByteRowKeyValueIterator(byte[] columnPosition, BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet, BitSet pkNotNullableBitSet) {
            super(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet, pkNotNullableBitSet);
            this.columnPosition = columnPosition;
        }
        
        @Override
        protected int getLength() {
            return columnPosition.length;
        }

        @Override
        protected int getPosition(int index) {
            return columnPosition[index];
        }

        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            output.write(columnPosition);
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + columnPosition.length;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            input.readFully(columnPosition);
        }

        @Override
        protected int setPosition(int index, int position) {
            return columnPosition[index] = (byte)position;
        }
    }
    
    private class IntRowKeyValueIterator extends RowKeyValueIterator {
        private final int[] columnPosition;
        
        IntRowKeyValueIterator(int[] columnPosition, BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet, BitSet pkNotNullableBitSet) {
            super(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet, pkNotNullableBitSet);
            this.columnPosition = columnPosition;
        }
        
        @Override
        protected int getLength() {
            return columnPosition.length;
        }

        @Override
        protected int getPosition(int index) {
            return columnPosition[index];
        }

        @Override
        protected int setPosition(int index, int position) {
            return columnPosition[index] = position;
        }
        @Override
        public void write(DataOutput output) throws IOException {
            for (int i = 0; i < columnPosition.length; i++) {
                output.writeInt(columnPosition[i]);
            }
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + columnPosition.length * Bytes.SIZEOF_INT;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            for (int i = 0; i < columnPosition.length; i++) {
                columnPosition[i] = input.readInt();
            }
        }
    }

    public byte[] buildRowKey(Map<ColumnReference, byte[]> valueMap, ImmutableBytesWritable rowKeyPtr) throws IOException {
        int estimatedSize = rowKeySchema.getEstimatedValueLength();
        for (int i = 0; i < indexedColumnByteSizes.size(); i++) {
            Integer byteSize = indexedColumnByteSizes.get(i);
            estimatedSize += byteSize == null ? ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE : byteSize;
        }
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedSize);
        DataOutput output = new DataOutputStream(stream);
        try {
            iterator.init(valueMap, rowKeyPtr);
            // Write index row key
            for (int i = 0; i < iterator.getLength(); i++) {
                if (!iterator.writeNextKeyPart(output)) {
                    output.write(QueryConstants.SEPARATOR_BYTE);
                }
            }
            int length = stream.size();
            byte[] indexRowKey = stream.getBuffer();
            // Remove trailing nulls
            while (indexRowKey[length-1] == QueryConstants.SEPARATOR_BYTE) {
                length--;
            }
            return indexRowKey.length == length ? indexRowKey : Arrays.copyOf(indexRowKey, length);
        } finally {
            stream.close();
        }
    }

    public List<ColumnReference> getCoverededColumns() {
        return coveredColumns;
    }

    public byte[] getEmptyKeyValueFamily() {
        // Since the metadata of an index table will never change,
        // we can infer this based on the family of the first covered column
        // If if there are no covered columns, we know it's our default name
        if (coveredColumns.isEmpty()) {
            return QueryConstants.EMPTY_COLUMN_BYTES;
        }
        return coveredColumns.get(0).getFamily();
    }

    public byte[] getIndexTableName() {
        return indexTableName;
    }
    
    
}
