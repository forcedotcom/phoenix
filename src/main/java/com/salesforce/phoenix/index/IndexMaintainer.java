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
import com.salesforce.phoenix.schema.ColumnModifier;
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

/**
 * 
 * Class that builds index row key from data row key and current state of
 * row and caches any covered columns. Client-side serializes into byte array using 
 * {@link #serialize(byte[], PTable, ImmutableBytesWritable)}
 * and transmits to server-side through either the 
 * {@link com.salesforce.phoenix.index.PhoenixIndexCodec#INDEX_MD}
 * Mutation attribute or as a separate RPC call using 
 * {@link com.salesforce.phoenix.cache.ServerCacheClient})
 *
 * @author jtaylor
 * @since 2.1.0
 */
public class IndexMaintainer implements Writable {
    
    /**
     * For client-side to serialize all IndexMaintainers for a given table
     * @param schemaName name of schema containing data table
     * @param dataTable data table
     * @param ptr bytes pointer to hold returned serialized value
     * @throws IOException 
     */
    public static void serialize(byte[] schemaName, PTable dataTable, ImmutableBytesWritable ptr) {
        List<PTable> indexes = dataTable.getIndexes();
        if (indexes.isEmpty()) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return;
        }
        int estimatedSize = 0;
        IndexMaintainer[] maintainers = new IndexMaintainer[indexes.size()];
        // TODO: handle salted column in data and/or index table
        for (int i = 0; i < indexes.size(); i++) {
            PTable index  = indexes.get(i);
            int nIndexColumns = index.getColumns().size();
            int nIndexPKColumns = index.getPKColumns().size();
            IndexMaintainer maintainer = new IndexMaintainer(
                    SchemaUtil.getTableName(schemaName, index.getName().getBytes()),
                    nIndexColumns,
                    nIndexPKColumns, 
                    dataTable.getRowKeySchema());
            maintainers[i] = maintainer;
            RowKeyMetaData iterator = maintainer.iterator();
            int j = 0;
            for (; j < nIndexPKColumns; j++) {
                PColumn indexColumn = index.getColumns().get(j);
                PColumn column = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
                boolean isPKColumn = SchemaUtil.isPKColumn(column);
                if (isPKColumn) {
                    iterator.setIndexPkPosition(dataTable.getPKColumns().indexOf(column), indexColumn.getPosition());
                    if (!column.isNullable()) {
                        iterator.getPkNotNullableBitSet().set(indexColumn.getPosition());
                    }
                    if (column.getColumnModifier() != null) {
                        iterator.getDescDataColumnBitSet().set(indexColumn.getPosition());
                    }
                } else {
                    maintainer.getIndexedColumnTypes().add(column.getDataType());
                    maintainer.getIndexedColumnSizes().add(column.getByteSize());
                    maintainer.getIndexedColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
                }
                if (indexColumn.getColumnModifier() != null) {
                    iterator.getDescIndexColumnBitSet().set(indexColumn.getPosition());
                }
            }
            for (; j < nIndexColumns; j++) {
                PColumn indexColumn = index.getColumns().get(j);
                PColumn column = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
                maintainer.getCoverededColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
            }
            estimatedSize += maintainer.getEstimatedByteSize();
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
    
    public static List<IndexMaintainer> deserialize(ImmutableBytesWritable metaDataPtr) {
        return deserialize(metaDataPtr.get(),metaDataPtr.getOffset(),metaDataPtr.getLength());
    }
    
    public static List<IndexMaintainer> deserialize(byte[] buf) {
        return deserialize(buf, 0, buf.length);
    }

    public static List<IndexMaintainer> deserialize(byte[] buf, int offset, int length) {
        ByteArrayInputStream stream = new ByteArrayInputStream(buf, offset, length);
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

    private List<ColumnReference> indexedColumns;
    private List<ColumnReference> coveredColumns;
    private List<ColumnReference> allColumns;
    private List<PDataType> indexedColumnTypes;
    private List<Integer> indexedColumnByteSizes;
    private RowKeySchema rowKeySchema;
    private RowKeyMetaData rowKeyMetaData;
    private byte[] indexTableName;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    private int estimatedIndexRowKeyBytes;
    protected int[][] dataRowKeyLocator;
    protected int[] dataPkPosition;
    
    private IndexMaintainer() {
    }

    private IndexMaintainer(byte[] indexTableName, int nIndexColumns, int nIndexPKColumns, RowKeySchema dataRowKeySchema) {
        int nDataPKColumns = dataRowKeySchema.getFieldCount();
        this.dataRowKeyLocator = new int[2][nIndexPKColumns];
        this.indexTableName = indexTableName;
        this.indexedColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.indexedColumnTypes = Lists.<PDataType>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.indexedColumnByteSizes = Lists.<Integer>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.coveredColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(nIndexColumns-nIndexPKColumns);
        this.allColumns = Lists.newArrayListWithExpectedSize(nDataPKColumns + nIndexColumns);
        this.allColumns.addAll(indexedColumns);
        this.allColumns.addAll(coveredColumns);
        this.rowKeySchema = dataRowKeySchema;
        this.rowKeyMetaData = newRowKeyMetaData(nIndexPKColumns);
    }

    public byte[] buildRowKey(Map<ColumnReference, byte[]> valueMap, ImmutableBytesWritable rowKeyPtr) throws IOException {
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes);
        DataOutput output = new DataOutputStream(stream);
        try {
            ptr.set(rowKeyPtr.get(), rowKeyPtr.getOffset(), rowKeyPtr.getLength());
            
            int i = 0;
            int nIndexedColumns = getIndexPkColumnCount();
            int maxRowKeyOffset = rowKeyPtr.getOffset() + rowKeyPtr.getLength();
            // Write index row key
            // TODO: investigate bug in rowKeySchema.setAccessor causing it not to skip over separator bytes
            for (Boolean hasValue = rowKeySchema.first(ptr, i, ValueBitSet.EMPTY_VALUE_BITSET); i < rowKeySchema.getFieldCount(); hasValue=rowKeySchema.next(ptr, ++i, maxRowKeyOffset, ValueBitSet.EMPTY_VALUE_BITSET)) {
                int pos = rowKeyMetaData.getIndexPkPosition(i);
                if (Boolean.TRUE.equals(hasValue)) {
                    dataRowKeyLocator[0][pos] = ptr.getOffset();
                    dataRowKeyLocator[1][pos] = ptr.getLength();
                } else {
                    dataRowKeyLocator[0][pos] = 0;
                    dataRowKeyLocator[1][pos] = 0;
                }
            }
            BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
            BitSet descDataColumnBitSet = rowKeyMetaData.getDescDataColumnBitSet();
            BitSet pkNotNullBitSet = rowKeyMetaData.getPkNotNullableBitSet();
            int j = 0;
            for (i = 0; i < nIndexedColumns; i++) {
                PDataType dataColumnType;
                boolean isNullable = true;
                boolean isDataColumnInverted = false;
                ColumnModifier dataColumnModifier = null;
                if (dataPkPosition[i] == -1) {
                    dataColumnType = indexedColumnTypes.get(j);
                    byte[] value = valueMap.get(indexedColumns.get(j));
                    if (value == null) {
                        ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                    } else {
                        ptr.set(value);
                    }
                    j++;
               } else {
                   dataColumnType = rowKeySchema.getField(dataPkPosition[i]).getType();
                   ptr.set(rowKeyPtr.get(), dataRowKeyLocator[0][i], dataRowKeyLocator[1][i]);
                    isDataColumnInverted = descDataColumnBitSet.get(i);
                    dataColumnModifier = isDataColumnInverted ? ColumnModifier.SORT_DESC : null;
                    isNullable = !pkNotNullBitSet.get(i);
                }
                PDataType indexColumnType = IndexUtil.getIndexColumnDataType(isNullable, dataColumnType);
                boolean isBytesComparable = dataColumnType.isBytesComparableWith(indexColumnType) ;
                if (isBytesComparable && isDataColumnInverted == descIndexColumnBitSet.get(i)) {
                    output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                } else {
                    if (!isBytesComparable)  {
                        indexColumnType.coerceBytes(ptr, dataColumnType, dataColumnModifier, null);
                    }
                    if (descIndexColumnBitSet.get(i) != isDataColumnInverted) {
                        writeInverted(ptr.get(), ptr.getOffset(), ptr.getLength(), output);
                    } else {
                        output.write(ptr.get(), ptr.getOffset(), ptr.getLength());
                    }
                }
                if (!indexColumnType.isFixedWidth()) {
                    output.writeByte(QueryConstants.SEPARATOR_BYTE);
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
    
    
    public List<ColumnReference> getIndexedColumns() {
        return indexedColumns;
    }

    public List<ColumnReference> getAllColumns() {
        return allColumns;
    }
    
    private RowKeyMetaData iterator() {
        return rowKeyMetaData;
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
            indexedColumns.add(new ColumnReference(cf,cq));
        }
        indexedColumnTypes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            PDataType type = PDataType.values()[WritableUtils.readVInt(input)];
            indexedColumnTypes.add(type);
        }
        estimatedIndexRowKeyBytes = 0;
        indexedColumnByteSizes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            int byteSize = WritableUtils.readVInt(input);
            Integer byteSizeInt;
            if (byteSize == 0) {
                byteSizeInt = null;
                byteSize = ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE;
            } else {
                byteSizeInt = byteSize;
            }
            estimatedIndexRowKeyBytes += byteSize;
            indexedColumnByteSizes.add(byteSizeInt);
        }
        int nCoveredColumns = WritableUtils.readVInt(input);
        coveredColumns = Lists.newArrayListWithExpectedSize(nCoveredColumns);
        for (int i = 0; i < nCoveredColumns; i++) {
            byte[] cf = Bytes.readByteArray(input);
            byte[] cq = Bytes.readByteArray(input);
            coveredColumns.add(new ColumnReference(cf,cq));
        }
        this.allColumns = Lists.newArrayListWithExpectedSize(indexedColumns.size() + coveredColumns.size());
        allColumns.addAll(indexedColumns);
        allColumns.addAll(coveredColumns);
        rowKeySchema = new RowKeySchema();
        rowKeySchema.readFields(input);
        estimatedIndexRowKeyBytes += rowKeySchema.getEstimatedValueLength();
        rowKeyMetaData = newRowKeyMetaData();
        rowKeyMetaData.readFields(input);

        int nIndexPkColumns = getIndexPkColumnCount();
        dataPkPosition = new int[nIndexPkColumns];
        Arrays.fill(dataPkPosition, -1);
        for (int i = 0; i < rowKeySchema.getFieldCount(); i++) {
            int dataPkPosition = rowKeyMetaData.getIndexPkPosition(i);
            this.dataPkPosition[dataPkPosition] = i;
        }
        dataRowKeyLocator = new int[2][nIndexPkColumns];
    }

    private int getIndexPkColumnCount() {
        return rowKeySchema.getFieldCount() + indexedColumns.size();
    }
    
    private RowKeyMetaData newRowKeyMetaData() {
        return getIndexPkColumnCount() < 0xFF ? new ByteSizeRowKeyMetaData() : new IntSizedRowKeyMetaData();
    }

    private RowKeyMetaData newRowKeyMetaData(int capacity) {
        return capacity <= 0xFF ? new ByteSizeRowKeyMetaData(capacity) : new IntSizedRowKeyMetaData(capacity);
    }

    private int getEstimatedByteSize() {
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
        size += rowKeyMetaData.getByteSize();
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
        rowKeyMetaData.write(output);
    }

    private static void writeInverted(byte[] buf, int offset, int length, DataOutput output) throws IOException {
        for (int i = offset; i < offset + length; i++) {
            byte b = ColumnModifier.SORT_DESC.apply(buf[i]);
            output.write(b);
        }
    }
    
    private abstract class RowKeyMetaData implements Writable {
        private BitSet descDataColumnBitSet;
        private BitSet descIndexColumnBitSet;
        private BitSet pkNotNullableBitSet;
        
        private RowKeyMetaData() {
        }
        
        private RowKeyMetaData(int nIndexedColumns) {
            descDataColumnBitSet = BitSet.withCapacity(nIndexedColumns);
            descIndexColumnBitSet = BitSet.withCapacity(nIndexedColumns);
            pkNotNullableBitSet = BitSet.withCapacity(nIndexedColumns);
      }
        
        protected int getByteSize() {
            return BitSet.getByteSize(getIndexPkColumnCount()) * 3;
        }
        
        protected abstract int getIndexPkPosition(int dataPkPosition);
        protected abstract int setIndexPkPosition(int dataPkPosition, int indexPkPosition);
        
        @Override
        public void readFields(DataInput input) throws IOException {
            int length = getIndexPkColumnCount();
            descDataColumnBitSet = BitSet.read(input, length);
            descIndexColumnBitSet = BitSet.read(input, length);
            pkNotNullableBitSet = BitSet.read(input, length);
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            int length = getIndexPkColumnCount();
            BitSet.write(output, descDataColumnBitSet, length);
            BitSet.write(output, descIndexColumnBitSet, length);
            BitSet.write(output, pkNotNullableBitSet, length);
        }

        private BitSet getDescDataColumnBitSet() {
            return descDataColumnBitSet;
        }

        private BitSet getDescIndexColumnBitSet() {
            return descIndexColumnBitSet;
        }
        
        private BitSet getPkNotNullableBitSet() {
            return pkNotNullableBitSet;
        }
    }
    
    private static int BYTE_OFFSET = 127;
    
    private class ByteSizeRowKeyMetaData extends RowKeyMetaData {
        private byte[] indexPkPosition;
        
        private ByteSizeRowKeyMetaData() {
        }

        private ByteSizeRowKeyMetaData(int nIndexedColumns) {
            super(nIndexedColumns);
            this.indexPkPosition = new byte[nIndexedColumns];
        }
        
        @Override
        protected int getIndexPkPosition(int dataPkPosition) {
            // Use offset for byte so that we can get full range of 0 - 255
            // We use -128 as marker for a non row key index column,
            // that's why our offset if 127 instead of 128
            return this.indexPkPosition[dataPkPosition] + BYTE_OFFSET;
        }

        @Override
        protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
            return this.indexPkPosition[dataPkPosition] = (byte)(indexPkPosition - BYTE_OFFSET);
        }

        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            output.write(indexPkPosition);
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + indexPkPosition.length;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            this.indexPkPosition = new byte[getIndexPkColumnCount()];
            input.readFully(indexPkPosition);
        }
    }
    
    private class IntSizedRowKeyMetaData extends RowKeyMetaData {
        private int[] indexPkPosition;
        
        private IntSizedRowKeyMetaData() {
        }

        private IntSizedRowKeyMetaData(int nIndexedColumns) {
            super(nIndexedColumns);
            this.indexPkPosition = new int[nIndexedColumns];
        }
        
        @Override
        protected int getIndexPkPosition(int dataPkPosition) {
            return this.indexPkPosition[dataPkPosition];
        }

        @Override
        protected int setIndexPkPosition(int dataPkPosition, int indexPkPosition) {
            return this.indexPkPosition[dataPkPosition] = indexPkPosition;
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            super.write(output);
            for (int i = 0; i < indexPkPosition.length; i++) {
                output.writeInt(indexPkPosition[i]);
            }
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + indexPkPosition.length * Bytes.SIZEOF_INT;
        }

        @Override
        public void readFields(DataInput input) throws IOException {
            super.readFields(input);
            this.indexPkPosition = new int[getIndexPkColumnCount()];
            for (int i = 0; i < indexPkPosition.length; i++) {
                indexPkPosition[i] = input.readInt();
            }
        }
    }
}
