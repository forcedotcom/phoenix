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
package com.salesforce.phoenix.execute;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.iterate.ParallelIterators.ParallelIteratorFactory;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.FilterableStatement;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.*;
import com.salesforce.phoenix.util.ScanUtil;
import com.salesforce.phoenix.util.SchemaUtil;



/**
 *
 * Query plan that has no child plans
 *
 * @author jtaylor
 * @since 0.1
 */
public abstract class BasicQueryPlan implements QueryPlan {
    protected final TableRef tableRef;
    protected final StatementContext context;
    protected final FilterableStatement statement;
    protected final RowProjector projection;
    protected final ParameterMetaData paramMetaData;
    protected final Integer limit;
    protected final OrderBy orderBy;
    protected final GroupBy groupBy;
    protected final ParallelIteratorFactory parallelIteratorFactory;

    private Scanner scanner;

    protected BasicQueryPlan(
            StatementContext context, FilterableStatement statement, TableRef table,
            RowProjector projection, ParameterMetaData paramMetaData, Integer limit, OrderBy orderBy,
            GroupBy groupBy, ParallelIteratorFactory parallelIteratorFactory) {
        this.context = context;
        this.statement = statement;
        this.tableRef = table;
        this.projection = projection;
        this.paramMetaData = paramMetaData;
        this.limit = limit;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.parallelIteratorFactory = parallelIteratorFactory;
    }

    @Override
    public GroupBy getGroupBy() {
        return groupBy;
    }

    
    @Override
    public OrderBy getOrderBy() {
        return orderBy;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    @Override
    public Integer getLimit() {
        return limit;
    }

    @Override
    public RowProjector getProjector() {
        return projection;
    }

    private ConnectionQueryServices getConnectionQueryServices(ConnectionQueryServices services) {
        byte[] tenantId = context.getConnection().getTenantId();
        // Get child services associated with tenantId of query.
        ConnectionQueryServices childServices = tenantId == null ? services : services.getChildQueryServices(new ImmutableBytesWritable(tenantId));
        return childServices;
    }

    protected void projectEmptyKeyValue() {
        Scan scan = context.getScan();
        PTable table = tableRef.getTable();
        if (!projection.isProjectEmptyKeyValue() && table.getType() != PTableType.VIEW) {
                scan.addColumn(SchemaUtil.getEmptyColumnFamily(table.getColumnFamilies()), QueryConstants.EMPTY_COLUMN_BYTES);
        }
    }
//    /**
//     * Sets up an id used to do round robin queue processing on the server
//     * @param scan
//     */
//    private void setProducer(Scan scan) {
//        byte[] producer = Bytes.toBytes(UUID.randomUUID().toString());
//        scan.setAttribute(HBaseServer.CALL_QUEUE_PRODUCER_ATTRIB_NAME, producer);
//    }

    @Override
    public final Scanner getScanner() throws SQLException {
        if (scanner != null) {
            return scanner;
        }
        Scan scan = context.getScan();
        // Set producer on scan so HBase server does round robin processing
        //setProducer(scan);
        // Set the time range on the scan so we don't get back rows newer than when the statement was compiled
        // The time stamp comes from the server at compile time when the meta data
        // is resolved.
        // TODO: include time range in explain plan?
        PhoenixConnection connection = context.getConnection();
        Long scn = connection.getSCN();
        ScanUtil.setTimeRange(scan, scn == null ? context.getCurrentTime() : scn);
        ScanUtil.setTenantId(scan, connection.getTenantId());
        scanner = newScanner();
        return scanner;
    }

    abstract protected Scanner newScanner(ConnectionQueryServices services) throws SQLException;

    private Scanner newScanner() throws SQLException {
        ConnectionQueryServices services = getConnectionQueryServices(context.getConnection().getQueryServices());
        if (context.getScanRanges() == ScanRanges.NOTHING) { // is degenerate
            scanner = new DegenerateScanner(tableRef, getProjector());
        } else {
            scanner = newScanner(services);
        }
        return scanner;
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return paramMetaData;
    }

    @Override
    public StatementContext getContext() {
        return context;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        if (scanner == null) {
            scanner = newScanner();
        }
        return scanner.getExplainPlan();
    }
}
