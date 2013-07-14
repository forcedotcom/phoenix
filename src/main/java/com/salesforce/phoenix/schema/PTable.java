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

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;

import com.salesforce.phoenix.schema.stat.PTableStats;


/**
 * Definition of a Phoenix table
 *
 * @author wmacklem,jtaylor
 * @since 0.1
 */
public interface PTable extends Writable {
    public static final long INITIAL_SEQ_NUM = 0;
    public static final String IS_IMMUTABLE_ROWS_PROP_NAME = "IMMUTABLE_ROWS";

    long getTimeStamp();
    long getSequenceNumber();
    /**
     * @return table name
     */
    PName getName();

    /**
     * @return the table type
     */
    PTableType getType();

    PName getPKName();

    /**
     * Get the PK columns ordered by position.
     * @return a list of the PK columns
     */
    List<PColumn> getPKColumns();

    /**
     * Get all columns ordered by position.
     * @return a list of all columns
     */
    List<PColumn> getColumns();

    /**
     * @return A list of the column families of this table
     *  ordered by position.
     */
    List<PColumnFamily> getColumnFamilies();

    /**
     * Get the column family with the given name
     * @param family the column family name
     * @return the PColumnFamily with the given name
     * @throws ColumnFamilyNotFoundException if the column family cannot be found
     */
    PColumnFamily getColumnFamily(byte[] family) throws ColumnFamilyNotFoundException;

    PColumnFamily getColumnFamily(String family) throws ColumnFamilyNotFoundException;

    /**
     * Get the column with the given string name.
     * @param name the column name
     * @return the PColumn with the given name
     * @throws ColumnNotFoundException if no column with the given name
     * can be found
     * @throws AmbiguousColumnException if multiple columns are found with the given name
     */
    PColumn getColumn(String name) throws ColumnNotFoundException, AmbiguousColumnException;

    /**
     * Get the PK column with the given name.
     * @param name the column name
     * @return the PColumn with the given name
     * @throws ColumnNotFoundException if no PK column with the given name
     * can be found
     * @throws SQLException 
     */
    PColumn getPKColumn(String name) throws ColumnNotFoundException;

    /**
     * Creates a new row at the specified timestamp using the key
     * for the PK values (from {@link #newKey(ImmutableBytesWritable, byte[][])}
     * and the optional key values specified using values.
     * @param ts the timestamp that the key value will have when committed
     * @param key the row key of the key value
     * @param values the optional key values
     * @return the new row. Use {@link com.salesforce.phoenix.schema.PRow#toRowMutations()} to
     * generate the Row to send to the HBase server.
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    PRow newRow(long ts, ImmutableBytesWritable key, byte[]... values);

    /**
     * Creates a new row for the PK values (from {@link #newKey(ImmutableBytesWritable, byte[][])}
     * and the optional key values specified using values. The timestamp of the key value
     * will be set by the HBase server.
     * @param key the row key of the key value
     * @param values the optional key values
     * @return the new row. Use {@link com.salesforce.phoenix.schema.PRow#toRowMutations()} to
     * generate the row to send to the HBase server.
     * @throws ConstraintViolationException if row data violates schema
     * constraint
     */
    PRow newRow(ImmutableBytesWritable key, byte[]... values);

    /**
     * Formulates a row key using the values provided. The values must be in
     * the same order as {@link #getPKColumns()}.
     * @param key bytes pointer that will be filled in with the row key
     * @param values the PK column values
     * @return the number of values that were used from values to set
     * the row key
     */
    int newKey(ImmutableBytesWritable key, byte[][] values);

    /**
     * Return the statistics table associated with this PTable.
     * @return the statistics table.
     */
    PTableStats getTableStats();

    RowKeySchema getRowKeySchema();

    /**
     * Return the number of buckets used by this table for salting. If the table does
     * not use salting, returns null.
     * @return number of buckets used by this table for salting, or null if salting is not used.
     */
    Integer getBucketNum();

    /**
     * Return the list of indexes defined on this table.
     * @return the list of indexes.
     */
    List<PTable> getIndexes();

    /**
     * For a table of index type, return the state of the table.
     * @return the state of the index.
     */
    PIndexState getIndexState();

    /**
     * For a table of index type, return the name of the data table.
     * @return the name of the data table that this index is on.
     */
    PName getDataTableName();
    boolean isImmutableRows();
}
