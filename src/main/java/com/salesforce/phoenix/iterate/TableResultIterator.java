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
package com.salesforce.phoenix.iterate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;


import com.google.common.io.Closeables;
import com.salesforce.phoenix.compile.StatementContext;
import com.salesforce.phoenix.exception.*;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.schema.tuple.Tuple;


/**
 * 
 * Wrapper for ResultScanner creation that closes HTableInterface
 * when ResultScanner is closed.
 *
 * @author jtaylor
 * @since 0.1
 */
public class TableResultIterator extends ExplainTable implements ResultIterator {
    private final HTableInterface htable;
    private final ResultIterator delegate;
    
    public TableResultIterator(StatementContext context, TableRef table) throws SQLException {
        this(context, table, context.getScan());
    }
    
    public TableResultIterator(StatementContext context, TableRef table, Scan scan) throws SQLException {
        super(context, table);
        htable = context.getConnection().getQueryServices().getTable(table.getTableName());
        try {
            delegate = new ScanningResultIterator(htable.getScanner(scan));
        } catch (IOException e) {
            Closeables.closeQuietly(htable);
            throw new PhoenixIOException(e);
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            delegate.close();
        } finally {
            try {
                htable.close();
            } catch (IOException e) {
                throw new PhoenixIOException(e);
            }
        }
    }

    @Override
    public Tuple next() throws SQLException {
        return delegate.next();
    }

    @Override
    public void explain(List<String> planSteps) {
        StringBuilder buf = new StringBuilder();
        explain(buf.toString(),planSteps);
    }
}
