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
package com.salesforce.phoenix.end2end;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.exception.*;
import com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData;
import com.salesforce.phoenix.query.*;


/**
 * 
 * Implementation of ConnectionQueryServices for tests running against
 * the mini cluster
 *
 * @author jtaylor
 * @since 0.1
 */
public class ConnectionQueryServicesTestImpl extends ConnectionQueryServicesImpl {
    private static HBaseTestingUtility util;

    public ConnectionQueryServicesTestImpl(Configuration config) throws SQLException {
        this(new QueryServicesTestImpl(config));
    }

    public ConnectionQueryServicesTestImpl(QueryServices services) throws SQLException {
        super(services, services.getConfig());
    }

    public ConnectionQueryServicesTestImpl(QueryServices services, Configuration config) throws SQLException {
        super(services, config);
    }
    
    private static Configuration setupServer(Configuration config) throws Exception {
        util = new HBaseTestingUtility(config);
        util.startMiniCluster();
        return util.getConfiguration();
    }
    
    private static void teardownServer() throws Exception {
        util.shutdownMiniCluster();
    }
    
    @Override
    public void init(String url, Properties props) throws SQLException {
        try {
            setupServer(this.getConfig());
            super.init(url, props);
            /**
             * Clear the server-side meta data cache on initialization. Otherwise, if we
             * query for meta data tables, we'll get nothing (since the server just came
             * up). However, our server-side cache (which is a singleton) will claim
             * that we do have tables and our create table calls will return the cached
             * meta data instead of creating new metadata.
             */
            SQLException sqlE = null;
            HTableInterface htable = this.getTable(PhoenixDatabaseMetaData.TYPE_TABLE_NAME);
            try {
                htable.coprocessorExec(MetaDataProtocol.class, HConstants.EMPTY_START_ROW,
                        HConstants.EMPTY_END_ROW, new Batch.Call<MetaDataProtocol, Void>() {
                    @Override
                    public Void call(MetaDataProtocol instance) throws IOException {
                      instance.clearCache();
                      return null;
                    }
                  });
            } catch (IOException e) {
                throw new PhoenixIOException(e);
            } catch (Throwable e) {
                sqlE = new SQLException(e);
            } finally {
                try {
                    htable.close();
                } catch (IOException e) {
                    if (sqlE == null) {
                        sqlE = new PhoenixIOException(e);
                    } else {
                        sqlE.setNextException(new PhoenixIOException(e));
                    }
                } finally {
                    if (sqlE != null) {
                        throw sqlE;
                    }
                }
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

        @Override
    public void close() throws SQLException {
        SQLException sqlE = null;
        try {
            super.close();
        } catch (SQLException e)  {
            sqlE = e;
        } finally {
            try {
                teardownServer();
            } catch (Exception e) {
                if (sqlE == null) {
                    sqlE = new SQLException(e);
                } else {
                    sqlE.setNextException(new SQLException(e));
                }
            } finally {
                if (sqlE != null) {
                    throw sqlE;
                }
            }
        }
    }
    
}
