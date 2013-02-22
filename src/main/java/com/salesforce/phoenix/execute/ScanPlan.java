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


import java.sql.SQLException;
import java.util.List;

import com.salesforce.phoenix.compile.OrderByCompiler.OrderBy;
import com.salesforce.phoenix.compile.*;
import com.salesforce.phoenix.coprocessor.ScanRegionObserver;
import com.salesforce.phoenix.iterate.*;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.schema.TableRef;



/**
 * 
 * Query plan for a basic table scan
 *
 * @author jtaylor
 * @since 0.1
 */
public class ScanPlan extends BasicQueryPlan {
    
    public ScanPlan(StatementContext context, TableRef table, RowProjector projection, Integer limit, OrderBy orderBy) {
        super(context, table, projection, context.getBindManager().getParameterMetaData(), limit, orderBy);
    }
    
    @Override
    public List<KeyRange> getSplits() {
        return null;
    }
    
    @Override
    public boolean isAggregate() {
        return false;
    }
    
    @Override
    protected Scanner newScanner(ConnectionQueryServices services) throws SQLException {
        ResultIterator scanner;
        // Either way, just use serial result iterator, instead of parallel one.
        // When we get the pre-fetching ClientScanner, this will be better, but even
        // without that, it's better not to run through the entire scan (which is
        // what the parallel iterator does) in case there are a billion rows and we'll
        // stop iterating after a few.
        if (limit == null) {
            scanner = new TableResultIterator(context, table);
            scanner = new SerialLimitingResultIterator(scanner, Long.MAX_VALUE, RowCounter.UNLIMIT_ROW_COUNTER);
        } else {
            scanner = new TableResultIterator(context, table);
            scanner = new SerialLimitingResultIterator(scanner, limit, new ScanRowCounter());
        }
        if (!orderBy.getOrderingColumns().isEmpty()) {
            scanner = new OrderedResultIterator(context, scanner, orderBy.getOrderingColumns());
        }
        context.getScan().setAttribute(ScanRegionObserver.NON_AGGREGATE_QUERY, QueryConstants.TRUE);

        return new WrappedScanner(scanner, getProjector());
    }
}
