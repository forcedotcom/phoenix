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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.Lists;
import com.salesforce.hbase.index.ValueGetter;
import com.salesforce.hbase.index.covered.update.ColumnReference;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.schema.SaltingUtil;
import com.salesforce.phoenix.schema.ValueSchema;
import com.salesforce.phoenix.schema.ValueSchema.Field;
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
    
    public static IndexMaintainer create(PTable dataTable, PTable index) {
        if (dataTable.getType() == PTableType.INDEX || index.getType() != PTableType.INDEX || !dataTable.getIndexes().contains(index)) {
            throw new IllegalArgumentException();
        }
        int indexPosOffset = index.getBucketNum() == null ? 0 : 1;
        int nIndexColumns = index.getColumns().size() - indexPosOffset;
        int nIndexPKColumns = index.getPKColumns().size() - indexPosOffset;
        IndexMaintainer maintainer = new IndexMaintainer(
                dataTable.getRowKeySchema(),
                dataTable.getBucketNum() != null,
                index.getName().getBytes(), 
                nIndexColumns,
                nIndexPKColumns,
                index.getBucketNum());
        RowKeyMetaData rowKeyMetaData = maintainer.getRowKeyMetaData();
        int j = indexPosOffset;
        for (; j < nIndexPKColumns + indexPosOffset; j++) {
            PColumn indexColumn = index.getColumns().get(j);
            assert(j == indexColumn.getPosition());
            int indexPos = j - indexPosOffset;
            PColumn column = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
            boolean isPKColumn = SchemaUtil.isPKColumn(column);
            if (isPKColumn) {
                int dataPkPos = dataTable.getPKColumns().indexOf(column) - (dataTable.getBucketNum() == null ? 0 : 1);
                rowKeyMetaData.setIndexPkPosition(dataPkPos, indexPos);
            } else {
                maintainer.getIndexedColumnTypes().add(column.getDataType());
                maintainer.getIndexedColumnSizes().add(column.getByteSize());
                maintainer.getIndexedColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
            }
            if (indexColumn.getColumnModifier() != null) {
                rowKeyMetaData.getDescIndexColumnBitSet().set(indexPos);
            }
        }
        for (; j < nIndexColumns; j++) {
            PColumn indexColumn = index.getColumns().get(j);
            PColumn column = IndexUtil.getDataColumn(dataTable, indexColumn.getName().getString());
            maintainer.getCoverededColumns().add(new ColumnReference(column.getFamilyName().getBytes(), column.getName().getBytes()));
        }
        maintainer.initCachedState();
        return maintainer;
    }
    
    /**
     * For client-side to serialize all IndexMaintainers for a given table
     * @param schemaName name of schema containing data table
     * @param dataTable data table
     * @param ptr bytes pointer to hold returned serialized value
     * @throws IOException 
     */
    public static void serialize(PTable dataTable, ImmutableBytesWritable ptr) {
        List<PTable> indexes = dataTable.getIndexes();
        if (dataTable.isImmutableRows() || indexes.isEmpty()) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            return;
        }
        int estimatedSize = dataTable.getRowKeySchema().getEstimatedByteSize() + 2;
        for (PTable index : indexes) {
            estimatedSize += index.getIndexMaintainer(dataTable).getEstimatedByteSize();
        }
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedSize + 1);
        DataOutput output = new DataOutputStream(stream);
        try {
            // Encode data table salting in sign of number of indexes
            WritableUtils.writeVInt(output, indexes.size() * (dataTable.getBucketNum() == null ? 1 : -1));
            // Write out data row key schema once, since it's the same for all index maintainers
            dataTable.getRowKeySchema().write(output);
            for (PTable index : indexes) {
                index.getIndexMaintainer(dataTable).write(output);
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
            boolean isDataTableSalted = size < 0;
            size = Math.abs(size);
            RowKeySchema rowKeySchema = new RowKeySchema();
            rowKeySchema.readFields(input);
            maintainers = Lists.newArrayListWithExpectedSize(size);
            for (int i = 0; i < size; i++) {
                IndexMaintainer maintainer = new IndexMaintainer(rowKeySchema, isDataTableSalted);
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
    private RowKeyMetaData rowKeyMetaData;
    private byte[] indexTableName;
    private int nIndexSaltBuckets;

    // Transient state
    private final boolean isDataTableSalted;
    private final RowKeySchema dataRowKeySchema;
    
    private List<byte[]> indexQualifiers;
    private int estimatedIndexRowKeyBytes;
    private int[][] dataRowKeyLocator;
    private int[] dataPkPosition;
    private int maxTrailingNulls;
    private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();
    
    private IndexMaintainer(RowKeySchema dataRowKeySchema, boolean isDataTableSalted) {
        this.dataRowKeySchema = dataRowKeySchema;
        this.isDataTableSalted = isDataTableSalted;
    }

    private IndexMaintainer(RowKeySchema dataRowKeySchema, boolean isDataTableSalted, byte[] indexTableName, int nIndexColumns, int nIndexPKColumns, Integer nIndexSaltBuckets) {
        this(dataRowKeySchema, isDataTableSalted);
        int nDataPKColumns = dataRowKeySchema.getFieldCount() - (isDataTableSalted ? 1 : 0);
        this.dataRowKeyLocator = new int[2][nIndexPKColumns];
        this.indexTableName = indexTableName;
        this.indexedColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.indexedColumnTypes = Lists.<PDataType>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.indexedColumnByteSizes = Lists.<Integer>newArrayListWithExpectedSize(nIndexPKColumns-nDataPKColumns);
        this.coveredColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(nIndexColumns-nIndexPKColumns);
        this.allColumns = Lists.newArrayListWithExpectedSize(nDataPKColumns + nIndexColumns);
        this.allColumns.addAll(indexedColumns);
        this.allColumns.addAll(coveredColumns);
        this.rowKeyMetaData = newRowKeyMetaData(nIndexPKColumns);
        this.nIndexSaltBuckets  = nIndexSaltBuckets == null ? 0 : nIndexSaltBuckets;
    }

    public byte[] buildRowKey(ValueGetter valueGetter, ImmutableBytesWritable rowKeyPtr)  {
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(estimatedIndexRowKeyBytes);
        DataOutput output = new DataOutputStream(stream);
        try {
            if (nIndexSaltBuckets > 0) {
                output.write(0); // will be set at end to index salt byte
            }
            
            // The dataRowKeySchema includes the salt byte field,
            // so we must adjust for that here.
            int dataPosOffset = isDataTableSalted ? 1 : 0 ;
            int nIndexedColumns = getIndexPkColumnCount();
            // Skip data table salt byte
            int maxRowKeyOffset = rowKeyPtr.getOffset() + rowKeyPtr.getLength();
            dataRowKeySchema.iterator(rowKeyPtr, ptr, dataPosOffset);
            // Write index row key
            for (int i = dataPosOffset; i < dataRowKeySchema.getFieldCount(); i++) {
                Boolean hasValue=dataRowKeySchema.next(ptr, i, maxRowKeyOffset);
                int pos = rowKeyMetaData.getIndexPkPosition(i-dataPosOffset);
                if (Boolean.TRUE.equals(hasValue)) {
                    dataRowKeyLocator[0][pos] = ptr.getOffset();
                    dataRowKeyLocator[1][pos] = ptr.getLength();
                } else {
                    dataRowKeyLocator[0][pos] = 0;
                    dataRowKeyLocator[1][pos] = 0;
                }
            }
            BitSet descIndexColumnBitSet = rowKeyMetaData.getDescIndexColumnBitSet();
            int j = 0;
            for (int i = 0; i < nIndexedColumns; i++) {
                PDataType dataColumnType;
                boolean isNullable = true;
                boolean isDataColumnInverted = false;
                ColumnModifier dataColumnModifier = null;
                if (dataPkPosition[i] == -1) {
                    dataColumnType = indexedColumnTypes.get(j);
                    byte[] value = valueGetter.getLatestValue(indexedColumns.get(j));
                    if (value == null) {
                        ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                    } else {
                        ptr.set(value);
                    }
                    j++;
               } else {
                   Field field = dataRowKeySchema.getField(dataPkPosition[i]);
                    dataColumnType = field.getDataType();
                    ptr.set(rowKeyPtr.get(), dataRowKeyLocator[0][i], dataRowKeyLocator[1][i]);
                    dataColumnModifier = field.getColumnModifier();
                    isDataColumnInverted = dataColumnModifier != null;
                    isNullable = field.isNullable();
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
            int minLength = length - maxTrailingNulls;
            byte[] indexRowKey = stream.getBuffer();
            // Remove trailing nulls
            while (length > minLength && indexRowKey[length-1] == QueryConstants.SEPARATOR_BYTE) {
                length--;
            }
            if (nIndexSaltBuckets > 0) {
                // Set salt byte
                byte saltByte = SaltingUtil.getSaltingByte(indexRowKey, SaltingUtil.NUM_SALTING_BYTES, length-SaltingUtil.NUM_SALTING_BYTES, nIndexSaltBuckets);
                indexRowKey[0] = saltByte;
            }
            return indexRowKey.length == length ? indexRowKey : Arrays.copyOf(indexRowKey, length);
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
    }

    // TODO: remove once Jesse handles a Put and Delete on the same row
    @SuppressWarnings("deprecation")
    public Put buildUpdateMutation(ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr) throws IOException {
        byte[] indexRowKey = this.buildRowKey(valueGetter, dataRowKeyPtr);
        Put put = new Put(indexRowKey);
        for (int i = 0; i < this.getCoverededColumns().size(); i++) {
            ColumnReference ref  = this.getCoverededColumns().get(i);
            byte[] iq = this.indexQualifiers.get(i);
            byte[] value = valueGetter.getLatestValue(ref);
            if (value == null) {
                // TODO: we should use a Delete here, but Jesse's framework can't handle that yet.
                // This will work, but will cause an otherwise sparse index to be bloated with empty
                // values for any unset covered columns.
                put.add(ref.getFamily(), iq, ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                put.add(ref.getFamily(), iq, value);
            }
        }
        // Add the empty key value
        put.add(this.getEmptyKeyValueFamily(), QueryConstants.EMPTY_COLUMN_BYTES, ByteUtil.EMPTY_BYTE_ARRAY);
        put.setWriteToWAL(false);
        return put;
    }

    public Pair<Put,Delete> buildUpdateMutations(ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr) throws IOException {
        return buildUpdateMutation(valueGetter, dataRowKeyPtr, HConstants.LATEST_TIMESTAMP);
    }
    
    @SuppressWarnings("deprecation")
    public Pair<Put,Delete> buildUpdateMutation(ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr, long ts) throws IOException {
        byte[] indexRowKey = this.buildRowKey(valueGetter, dataRowKeyPtr);
        Delete delete = null;
        Put put = new Put(indexRowKey);
        for (int i = 0; i < this.getCoverededColumns().size(); i++) {
            ColumnReference ref  = this.getCoverededColumns().get(i);
            byte[] iq = this.indexQualifiers.get(i);
            byte[] value = valueGetter.getLatestValue(ref);
            if (value == null) {
                if (delete == null) {
                    delete = new Delete(indexRowKey);
                    delete.setWriteToWAL(false);
                }
                delete.deleteColumns(ref.getFamily(), iq, ts);
            } else {
                put.add(ref.getFamily(), iq, ts, value);
            }
        }
        // Add the empty key value
        put.add(this.getEmptyKeyValueFamily(), QueryConstants.EMPTY_COLUMN_BYTES, ts, ByteUtil.EMPTY_BYTE_ARRAY);
        put.setWriteToWAL(false);
        return new Pair<Put,Delete>(put,delete);
    }
    
    public Delete buildDeleteMutation(ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr) throws IOException {
        return buildDeleteMutation(valueGetter, dataRowKeyPtr, HConstants.LATEST_TIMESTAMP);
    }
    
    @SuppressWarnings("deprecation")
    public Delete buildDeleteMutation(ValueGetter valueGetter, ImmutableBytesWritable dataRowKeyPtr, long ts) throws IOException {
        byte[] indexRowKey = this.buildRowKey(valueGetter, dataRowKeyPtr);
        Delete delete = new Delete(indexRowKey, ts, null);
        delete.setWriteToWAL(false);
        return delete;
    }

    public byte[] getIndexTableName() {
        return indexTableName;
    }
    
    public List<ColumnReference> getCoverededColumns() {
        return coveredColumns;
    }

    public List<ColumnReference> getIndexedColumns() {
        return indexedColumns;
    }

    public List<ColumnReference> getAllColumns() {
        return allColumns;
    }
    
    private byte[] getEmptyKeyValueFamily() {
        // Since the metadata of an index table will never change,
        // we can infer this based on the family of the first covered column
        // If if there are no covered columns, we know it's our default name
        if (coveredColumns.isEmpty()) {
            return QueryConstants.EMPTY_COLUMN_BYTES;
        }
        return coveredColumns.get(0).getFamily();
    }

    private RowKeyMetaData getRowKeyMetaData() {
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
        nIndexSaltBuckets = WritableUtils.readVInt(input);
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
        indexedColumnByteSizes = Lists.newArrayListWithExpectedSize(nIndexedColumns);
        for (int i = 0; i < nIndexedColumns; i++) {
            int byteSize = WritableUtils.readVInt(input);
            indexedColumnByteSizes.add(byteSize == 0 ? null : Integer.valueOf(byteSize));
        }
        int nCoveredColumns = WritableUtils.readVInt(input);
        coveredColumns = Lists.newArrayListWithExpectedSize(nCoveredColumns);
        for (int i = 0; i < nCoveredColumns; i++) {
            byte[] cf = Bytes.readByteArray(input);
            byte[] cq = Bytes.readByteArray(input);
            coveredColumns.add(new ColumnReference(cf,cq));
        }
        indexTableName = Bytes.readByteArray(input);
        rowKeyMetaData = newRowKeyMetaData();
        rowKeyMetaData.readFields(input);
        
        initCachedState();
    }
    
    private int estimateIndexRowKeyByteSize() {
        int estimatedIndexRowKeyBytes = dataRowKeySchema.getEstimatedValueLength() + (nIndexSaltBuckets == 0 ?  0 : SaltingUtil.NUM_SALTING_BYTES);
        for (Integer byteSize : indexedColumnByteSizes) {
            estimatedIndexRowKeyBytes += (byteSize == null ? ValueSchema.ESTIMATED_VARIABLE_LENGTH_SIZE : byteSize);
        }
        return estimatedIndexRowKeyBytes;
   }
    
    /**
     * Init calculated state reading/creating
     */
    private void initCachedState() {
        indexQualifiers = Lists.newArrayListWithExpectedSize(this.coveredColumns.size());
        for (int i = 0; i < coveredColumns.size(); i++) {
            ColumnReference ref = coveredColumns.get(i);
            indexQualifiers.add(IndexUtil.getIndexColumnName(ref.getFamily(), ref.getQualifier()));
        }
        estimatedIndexRowKeyBytes = estimateIndexRowKeyByteSize();

        this.allColumns = Lists.newArrayListWithExpectedSize(indexedColumns.size() + coveredColumns.size());
        allColumns.addAll(indexedColumns);
        allColumns.addAll(coveredColumns);
        
        int dataPkOffset = isDataTableSalted ? 1 : 0;
        int nIndexPkColumns = getIndexPkColumnCount();
        dataPkPosition = new int[nIndexPkColumns];
        Arrays.fill(dataPkPosition, -1);
        for (int i = dataPkOffset; i < dataRowKeySchema.getFieldCount(); i++) {
            int dataPkPosition = rowKeyMetaData.getIndexPkPosition(i-dataPkOffset);
            this.dataPkPosition[dataPkPosition] = i;
        }
        dataRowKeyLocator = new int[2][nIndexPkColumns];
        
        // Calculate the max number of trailing nulls that we should get rid of after building the index row key.
        // We only get rid of nulls for variable length types, so we have to be careful to consider the type of the
        // index table, not the data type of the data table
        int indexedColumnTypesPos = indexedColumnTypes.size()-1;
        int indexPkPos = nIndexPkColumns-1;
        while (indexPkPos >= 0) {
            int dataPkPos = dataPkPosition[indexPkPos];
            boolean isDataNullable;
            PDataType dataType;
            if (dataPkPos == -1) {
                isDataNullable = true;
                dataType = indexedColumnTypes.get(indexedColumnTypesPos--);
            } else {
                Field dataField = dataRowKeySchema.getField(dataPkPos);
                dataType = dataField.getDataType();
                isDataNullable = dataField.isNullable();
            }
            PDataType indexDataType = IndexUtil.getIndexColumnDataType(isDataNullable, dataType);
            if (indexDataType.isFixedWidth()) {
                break;
            }
            indexPkPos--;
        }
        maxTrailingNulls = nIndexPkColumns-indexPkPos-1;
    }

    private int getIndexPkColumnCount() {
        return dataRowKeySchema.getFieldCount() + indexedColumns.size() - (isDataTableSalted ? 1 : 0);
    }
    
    private RowKeyMetaData newRowKeyMetaData() {
        return getIndexPkColumnCount() < 0xFF ? new ByteSizeRowKeyMetaData() : new IntSizedRowKeyMetaData();
    }

    private RowKeyMetaData newRowKeyMetaData(int capacity) {
        return capacity < 0xFF ? new ByteSizeRowKeyMetaData(capacity) : new IntSizedRowKeyMetaData(capacity);
    }

    public int getEstimatedByteSize() {
        int size = WritableUtils.getVIntSize(nIndexSaltBuckets);
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
        size += WritableUtils.getVIntSize(coveredColumns.size());
        for (int i = 0; i < coveredColumns.size(); i++) {
            ColumnReference ref = coveredColumns.get(i);
            size += WritableUtils.getVIntSize(ref.getFamily().length);
            size += ref.getFamily().length;
            size += WritableUtils.getVIntSize(ref.getQualifier().length);
            size += ref.getQualifier().length;
        }
        size += indexTableName.length + WritableUtils.getVIntSize(indexTableName.length);
        size += rowKeyMetaData.getByteSize();
        return size;
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        WritableUtils.writeVInt(output, nIndexSaltBuckets);
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
        Bytes.writeByteArray(output, indexTableName);
        rowKeyMetaData.write(output);
    }

    private static void writeInverted(byte[] buf, int offset, int length, DataOutput output) throws IOException {
        for (int i = offset; i < offset + length; i++) {
            byte b = ColumnModifier.SORT_DESC.apply(buf[i]);
            output.write(b);
        }
    }
    
    private abstract class RowKeyMetaData implements Writable {
        private BitSet descIndexColumnBitSet;
        
        private RowKeyMetaData() {
        }
        
        private RowKeyMetaData(int nIndexedColumns) {
            descIndexColumnBitSet = BitSet.withCapacity(nIndexedColumns);
      }
        
        protected int getByteSize() {
            return BitSet.getByteSize(getIndexPkColumnCount()) * 3;
        }
        
        protected abstract int getIndexPkPosition(int dataPkPosition);
        protected abstract int setIndexPkPosition(int dataPkPosition, int indexPkPosition);
        
        @Override
        public void readFields(DataInput input) throws IOException {
            int length = getIndexPkColumnCount();
            descIndexColumnBitSet = BitSet.read(input, length);
        }
        
        @Override
        public void write(DataOutput output) throws IOException {
            int length = getIndexPkColumnCount();
            BitSet.write(output, descIndexColumnBitSet, length);
        }

        private BitSet getDescIndexColumnBitSet() {
            return descIndexColumnBitSet;
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
