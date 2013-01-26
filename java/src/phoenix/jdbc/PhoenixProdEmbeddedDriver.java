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
package phoenix.jdbc;

import static phoenix.query.QueryServicesOptions.withDefaults;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import phoenix.query.ConnectionQueryServices;
import phoenix.query.ConnectionQueryServicesImpl;
import phoenix.query.ConnectionlessQueryServicesImpl;
import phoenix.query.QueryServicesImpl;
import phoenix.util.SQLCloseables;

/**
 * 
 * JDBC Driver implementation of Phoenix for production.
 * To use this driver, specify the following URL:
 *     jdbc:phoenix:<zookeeper quorum server name>;
 * Only an embedded driver is currently supported (Phoenix client
 * runs in the same JVM as the driver). Connections are lightweight
 * and are not pooled. The last part of the URL, the hbase zookeeper
 * quorum server name, determines the hbase cluster to which queries
 * will be routed.
 * 
 * @author jtaylor
 * @since 0.1
 */
public final class PhoenixProdEmbeddedDriver extends PhoenixEmbeddedDriver {
    private static final String ZOOKEEPER_QUARUM_ATTRIB = "hbase.zookeeper.quorum";
    public static final PhoenixProdEmbeddedDriver INSTANCE;
    static {
        try {
            DriverManager.registerDriver( INSTANCE = new PhoenixProdEmbeddedDriver() );
        } catch (SQLException e) {
            throw new IllegalStateException("Untable to register PhoenixProdEmbeddedDriver: " + e.getMessage());
        }
    }
    private final ConcurrentMap<String,ConnectionQueryServices> connectionQueryServicesMap = new ConcurrentHashMap<String,ConnectionQueryServices>(3);

    public PhoenixProdEmbeddedDriver() { // for Squirrel
        // Use production services implementation
        super(new QueryServicesImpl(withDefaults(HBaseConfiguration.create())));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Accept the url only if test=true attribute set
        return super.acceptsURL(url) && !url.contains(";test=true");
    }

    // TODO: need a way to replace existing ConnectionQueryServices with new one with modified PMetaData
    @Override
    protected ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
        String serverName = getZookeeperQuorum(url);
        ConnectionQueryServices connectionQueryServices = connectionQueryServicesMap.get(serverName);
        if (connectionQueryServices == null) {
            if (CONNECTIONLESS.equals(serverName)) {
                connectionQueryServices = new ConnectionlessQueryServicesImpl(getQueryServices());
            } else {
                Configuration childConfig = HBaseConfiguration.create(getQueryServices().getConfig());
                childConfig.set(ZOOKEEPER_QUARUM_ATTRIB, serverName);
                connectionQueryServices = new ConnectionQueryServicesImpl(getQueryServices(), childConfig);
            }
            connectionQueryServices.init(url, info);
            ConnectionQueryServices prevValue = connectionQueryServicesMap.putIfAbsent(serverName, connectionQueryServices);
            if (prevValue != null) {
                connectionQueryServices = prevValue;
            }
        }
        return connectionQueryServices;
    }

    @Override
    public void close() throws SQLException {
        try {
            SQLCloseables.closeAll(connectionQueryServicesMap.values());
        } finally {
            connectionQueryServicesMap.clear();            
        }
    }
}
