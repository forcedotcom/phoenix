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
package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.text.Format;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.BindableStatement;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.RowKeySchema;
import com.salesforce.phoenix.util.ByteUtil;
import com.salesforce.phoenix.util.DateUtil;
import com.salesforce.phoenix.util.NumberUtil;


/**
 *
 * Class that keeps common state used across processing the various clauses in a
 * top level JDBC statement such as SELECT, UPSERT, DELETE, etc.
 *
 * @author jtaylor
 * @since 0.1
 */
public class StatementContext {
    private final ColumnResolver resolver;
    private final BindManager binds;
    private final Scan scan;
    private final ExpressionManager expressions;
    private final AggregationManager aggregates;
    private final String dateFormat;
    private final Format dateFormatter;
    private final Format dateParser;
    private final String numberFormat;
    private final ImmutableBytesWritable tempPtr;
    private final PhoenixConnection connection;
    
    private long currentTime = QueryConstants.UNSET_TIMESTAMP;
    private ScanRanges scanRanges = ScanRanges.EVERYTHING;
    private KeyRange minMaxRange = null;

    public StatementContext(BindableStatement statement, PhoenixConnection connection, ColumnResolver resolver, List<Object> binds, Scan scan) {
        this.connection = connection;
        this.resolver = resolver;
        this.scan = scan;
        this.binds = new BindManager(binds, statement.getBindCount());
        this.aggregates = new AggregationManager();
        this.expressions = new ExpressionManager();
        this.dateFormat = connection.getQueryServices().getProps().get(QueryServices.DATE_FORMAT_ATTRIB, DateUtil.DEFAULT_DATE_FORMAT);
        this.dateFormatter = DateUtil.getDateFormatter(dateFormat);
        this.dateParser = DateUtil.getDateParser(dateFormat);
        this.numberFormat = connection.getQueryServices().getProps().get(QueryServices.NUMBER_FORMAT_ATTRIB, NumberUtil.DEFAULT_NUMBER_FORMAT);
        this.tempPtr = new ImmutableBytesWritable();
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public Format getDateFormatter() {
        return dateFormatter;
    }

    public Format getDateParser() {
        return dateParser;
    }
    
    public String getNumberFormat() {
        return numberFormat;
    }
    
    public Scan getScan() {
        return scan;
    }

    public BindManager getBindManager() {
        return binds;
    }

    public AggregationManager getAggregationManager() {
        return aggregates;
    }

    public ColumnResolver getResolver() {
        return resolver;
    }

    public ExpressionManager getExpressionManager() {
        return expressions;
    }


    public ImmutableBytesWritable getTempPtr() {
        return tempPtr;
    }

    public ScanRanges getScanRanges() {
        return this.scanRanges;
    }
    
    public void setScanRanges(ScanRanges scanRanges) {
        this.scanRanges = scanRanges;
        this.scanRanges.setScanStartStopRow(scan);
        PTable table = this.getResolver().getTables().get(0).getTable();
        if (table.getBucketNum() == null && minMaxRange != null) { 
            KeyRange range = KeyRange.getKeyRange(scan.getStartRow(), scan.getStopRow());
            // TODO: util for this: ScanUtil.toLowerInclusiveUpperExclusiveRange
            range = range.intersect(minMaxRange);
            if (!range.lowerUnbound()) {
                byte[] lowerRange = range.getLowerRange();
                if (!range.isLowerInclusive()) {
                    // Find how slots the minMaxRange spans
                    int pos = 0;
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    RowKeySchema schema = table.getRowKeySchema();
                    int maxOffset = schema.iterator(lowerRange, ptr);
                    while (schema.next(ptr, pos, maxOffset) != null) {
                        pos++;
                    }
                    if (!schema.getField(pos-1).getDataType().isFixedWidth()) {
                        byte[] newLowerRange = new byte[lowerRange.length + 1];
                        System.arraycopy(lowerRange, 0, newLowerRange, 0, lowerRange.length);
                        newLowerRange[lowerRange.length] = QueryConstants.SEPARATOR_BYTE;
                        lowerRange = newLowerRange;
                    } else {
                        lowerRange = Arrays.copyOf(lowerRange, lowerRange.length);
                    }
                    ByteUtil.nextKey(lowerRange, lowerRange.length);
                }
                scan.setStartRow(lowerRange);
            }
            
            byte[] upperRange = range.getUpperRange();
            if (!range.upperUnbound()) {
                if (range.isUpperInclusive()) {
                    // Find how slots the minMaxRange spans
                    int pos = 0;
                    ImmutableBytesWritable ptr = new ImmutableBytesWritable();
                    RowKeySchema schema = table.getRowKeySchema();
                    int maxOffset = schema.iterator(upperRange, ptr);
                    while (schema.next(ptr, pos, maxOffset) != null) {
                        pos++;
                    }
                    if (!schema.getField(pos-1).getDataType().isFixedWidth()) {
                        byte[] newUpperRange = new byte[upperRange.length + 1];
                        System.arraycopy(upperRange, 0, newUpperRange, 0, upperRange.length);
                        newUpperRange[upperRange.length] = QueryConstants.SEPARATOR_BYTE;
                        upperRange = newUpperRange;
                    } else {
                        upperRange = Arrays.copyOf(upperRange, upperRange.length);
                    }
                    ByteUtil.nextKey(upperRange, upperRange.length);
                }
                scan.setStopRow(upperRange);
            }
        }
    }
    
    public PhoenixConnection getConnection() {
        return connection;
    }

    public long getCurrentTime() throws SQLException {
        long ts = this.getResolver().getTables().get(0).getTimeStamp();
        if (ts != QueryConstants.UNSET_TIMESTAMP) {
            return ts;
        }
        if (currentTime != QueryConstants.UNSET_TIMESTAMP) {
            return currentTime;
        }
        /*
         * For an UPSERT VALUES where autocommit off, we won't hit the server until the commit.
         * However, if the statement has a CURRENT_DATE() call as a value, we need to know the
         * current time at execution time. In that case, we'll call MetaDataClient.updateCache
         * purely to bind the current time based on the server time.
         */
        PTable table = this.getResolver().getTables().get(0).getTable();
        MetaDataClient client = new MetaDataClient(connection);
        currentTime = Math.abs(client.updateCache(table.getSchemaName().getString(), table.getTableName().getString()));
        return currentTime;
    }

    /**
     * Get the key range derived from row value constructor usage in where clause. These are orthogonal to the ScanRanges
     * and form a range for which each scan is intersected against.
     */
    public KeyRange getMinMaxRange () {
        return minMaxRange;
    }
    
    public void setMinMaxRange(KeyRange minMaxRange) {
        this.minMaxRange = minMaxRange;
    }
}
