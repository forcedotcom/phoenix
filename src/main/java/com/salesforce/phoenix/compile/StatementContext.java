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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.text.Format;
import java.util.List;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.filter.SkipScanFilter;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.KeyRange;
import com.salesforce.phoenix.query.QueryConstants;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.schema.MetaDataClient;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.DateUtil;


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
    private final ImmutableBytesWritable tempPtr;
    private final PhoenixConnection connection;
    private List<List<KeyRange>> cnf;
    private int[] widths;

    private boolean isAggregate;
    private ScanKey scanKey;
    private GroupBy groupBy;
    private long currentTime = QueryConstants.UNSET_TIMESTAMP;

    public StatementContext(PhoenixConnection connection, ColumnResolver resolver, List<Object> binds, int bindCount, Scan scan) {
        this.connection = connection;
        this.resolver = resolver;
        this.scan = scan;
        this.binds = new BindManager(binds, bindCount);
        this.aggregates = new AggregationManager();
        this.expressions = new ExpressionManager();
        this.dateFormat = connection.getQueryServices().getConfig().get(QueryServices.DATE_FORMAT_ATTRIB, DateUtil.DEFAULT_DATE_FORMAT);
        this.dateFormatter = DateUtil.getDateFormatter(dateFormat);
        this.dateParser = DateUtil.getDateParser(dateFormat);
        this.tempPtr = new ImmutableBytesWritable();
        this.scanKey = ScanKey.EVERYTHING_SCAN_KEY;
        this.groupBy = GroupBy.EMPTY_GROUP_BY;
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

    public ScanKey getScanKey() {
        return scanKey;
    }

    public void setCnf(List<List<KeyRange>> cnf, int[] widths) {
        this.cnf = cnf;
        this.widths = widths;
    }

    public boolean hasCnf() {
        return cnf != null && widths != null && !cnf.isEmpty();
    }

    /**
     * @return check for null
     */
    public SkipScanFilter newSkipScanFilter() {
        return new SkipScanFilter().setCnf(cnf, widths);
    }

    public void setScanKey(ScanKey scanKey) {
        this.scanKey = scanKey;
        scanKey.setScanStartStopKey(scan);
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
        TableRef table = this.getResolver().getTables().get(0);
        MetaDataClient client = new MetaDataClient(connection);
        currentTime = Math.abs(client.updateCache(table.getSchema().getName(), table.getTable().getName().getString()));
        return currentTime;
    }


    public void setAggregate(boolean isAggregate) {
        this.isAggregate = isAggregate;
    }


    public boolean isAggregate() {
        return isAggregate;
    }

    public GroupBy getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(GroupBy groupBy) {
        setAggregate(true);
        this.groupBy = groupBy;
        ByteArrayOutputStream stream = new ByteArrayOutputStream(Math.max(1, groupBy.getExpressions().size() * 10));
        try {
            if (groupBy.getExpressions().isEmpty()) {
                stream.write(QueryConstants.TRUE);
            } else {
                DataOutputStream output = new DataOutputStream(stream);
                for (Expression expression : groupBy.getKeyExpressions()) {
                    WritableUtils.writeVInt(output, ExpressionType.valueOf(expression).ordinal());
                    expression.write(output);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // Impossible
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        getScan().setAttribute(groupBy.getScanAttribName(), stream.toByteArray());

    }
}
