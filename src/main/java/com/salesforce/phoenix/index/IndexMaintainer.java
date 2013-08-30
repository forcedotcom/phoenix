package com.salesforce.phoenix.index;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.*;
import com.salesforce.hbase.index.builder.covered.ColumnReference;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.*;
import com.salesforce.phoenix.util.BitSet;

public class IndexMaintainer implements Writable {
    private List<ColumnReference> indexedColumns;
    private List<ColumnReference> coveredColumns;
    private List<ColumnReference> allColumns;
    private RowKeySchema rowKeySchema;
    private RowKeyValueIterator iterator;
    private byte[] indexTableName;
    
    public IndexMaintainer() {
    }

    public IndexMaintainer(byte[] indexTableName, List<ColumnReference> indexedColumns, List<ColumnReference> coveredColumns, RowKeySchema rowKeySchema, 
            BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet) {
        this.indexTableName = indexTableName;
        this.indexedColumns = indexedColumns;
        this.coveredColumns = coveredColumns;
        this.allColumns = Lists.newArrayListWithExpectedSize(indexedColumns.size() + coveredColumns.size());
        allColumns.addAll(indexedColumns);
        allColumns.addAll(coveredColumns);
        this.rowKeySchema = rowKeySchema;
        iterator = newIterator(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet);
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

    public static Multimap<ColumnReference, IndexMaintainer> deserialize(byte[] mdValue) {
        ByteArrayInputStream stream = new ByteArrayInputStream(mdValue);
        DataInput input = new DataInputStream(stream);
        Multimap<ColumnReference, IndexMaintainer> indexMap = null;
        try {
            int size = WritableUtils.readVInt(input);
            indexMap = ArrayListMultimap.create(size * 5, size);
            for (int i = 0; i < size; i++) {
                IndexMaintainer maintainer = new IndexMaintainer();
                maintainer.readFields(input);
                for (ColumnReference ref : maintainer.getAllColumns()) {
                    indexMap.put(ref, maintainer);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        }
        return indexMap;
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
            List<ColumnReference>indexedColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(totalIndexedColumns-nDataTablePKColumns);
            List<ColumnReference>coveredColumns = Lists.<ColumnReference>newArrayListWithExpectedSize(index.getColumns().size()-totalIndexedColumns);
            maintainers[i] = new IndexMaintainer(SchemaUtil.getTableName(schemaName, index.getName().getBytes()),
                    indexedColumns, coveredColumns, dataTable.getRowKeySchema(), 
                    pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet);
        }
        /*
         * Make one pass through all data columns. We need to lead with this, because
         * we have no unambiguous way currently of going from an index column back to
         * the data column. TODO: review, as this would be useful
         */
        int pkPosition = 0;
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
                        if (column.getColumnModifier() != null) {
                            iterator.getDescDataColumnBitSet().set(indexColumn.getPosition());
                        }
                    } else {
                        if (SchemaUtil.isPKColumn(indexColumn)) {
                            iterator.setPosition(indexColumn.getPosition(), maintainer.getIndexedColumns().size());
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
        iterator = newIterator(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet);
    }

    private RowKeyValueIterator newIterator(BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet) {
        int capacity = rowKeySchema.getFieldCount() + indexedColumns.size();
        return capacity <= 0xFF ? 
                new ByteRowKeyValueIterator(new byte[capacity], pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet) : 
                new IntRowKeyValueIterator(new int[capacity], pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet);
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
        rowKeySchema.write(output);
        int totalIndexedColumns = rowKeySchema.getFieldCount() + indexedColumns.size();
        BitSet.write(output, iterator.getPkColumnBitSet(), totalIndexedColumns);
        BitSet.write(output, iterator.getDescDataColumnBitSet(), totalIndexedColumns);
        BitSet.write(output, iterator.getDescIndexColumnBitSet(), totalIndexedColumns);
        iterator.write(output);
    }

    public abstract class RowKeyValueIterator {
        private final BitSet pkColumnBitSet;
        private final BitSet descDataColumnBitSet;
        private final BitSet descIndexColumnBitSet;
        
        private int index;
        
        RowKeyValueIterator(BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet) {
            this.pkColumnBitSet = pkColumnBitSet;
            this.descDataColumnBitSet = descDataColumnBitSet;
            this.descIndexColumnBitSet = descIndexColumnBitSet;
        }
        
        protected int getByteSize() {
            return BitSet.getByteSize(getLength()) * 3;
        }
        
        protected abstract int getLength();
        protected abstract int getPosition(int index);
        protected abstract int setPosition(int index, int position);
        protected abstract void read(DataInput input) throws IOException;
        protected abstract void write(DataOutput output) throws IOException;

        public boolean next() {
            if (index >= getLength()) {
                return false;
            }
            index++;
            
            return true;
        }

        public void init() {
            index = -1;
        }

        public void getValue(ImmutableBytesWritable rowKey, ImmutableBytesWritable ptr) {
            if (index < 0 || index >= getLength()) {
                throw new NoSuchElementException();
            }
            if (index == 0) {
            } else if (getPosition(index-1) < getPosition(index)) { // Increasing, then start from ptr
                
            } else { // Reposition from beginning
                
            }
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
    }
    
    private class ByteRowKeyValueIterator extends RowKeyValueIterator {
        private final byte[] columnPosition;
        
        ByteRowKeyValueIterator(byte[] columnPosition, BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet) {
            super(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet);
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
        protected void write(DataOutput output) throws IOException {
            output.write(columnPosition);
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + columnPosition.length;
        }

        @Override
        protected void read(DataInput input) throws IOException {
            input.readFully(columnPosition);
        }

        @Override
        protected int setPosition(int index, int position) {
            return columnPosition[index] = (byte)position;
        }
    }
    
    private class IntRowKeyValueIterator extends RowKeyValueIterator {
        private final int[] columnPosition;
        
        IntRowKeyValueIterator(int[] columnPosition, BitSet pkColumnBitSet, BitSet descDataColumnBitSet, BitSet descIndexColumnBitSet) {
            super(pkColumnBitSet, descDataColumnBitSet, descIndexColumnBitSet);
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
        protected void write(DataOutput output) throws IOException {
            for (int i = 0; i < columnPosition.length; i++) {
                output.writeInt(columnPosition[i]);
            }
        }

        @Override
        protected int getByteSize() {
            return super.getByteSize() + columnPosition.length * Bytes.SIZEOF_INT;
        }

        @Override
        protected void read(DataInput input) throws IOException {
            for (int i = 0; i < columnPosition.length; i++) {
                columnPosition[i] = input.readInt();
            }
        }
    }

    public byte[] buildRowKey(Map<ColumnReference, byte[]> valueMap) {
        // TODO Auto-generated method stub
        return null;
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
