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
package com.salesforce.phoenix.jdbc;

import static com.salesforce.phoenix.query.QueryServicesOptions.withDefaults;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.query.*;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.SQLCloseables;


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
public final class PhoenixDriver extends PhoenixEmbeddedDriver {
    private static final String ZOOKEEPER_QUARUM_ATTRIB = "hbase.zookeeper.quorum";
    private static final String ZOOKEEPER_PORT_ATTRIB = "hbase.zookeeper.property.clientPort";
    private static final String ZOOKEEPER_ROOT_NODE_ATTRIB = "zookeeper.znode.parent";
    public static final PhoenixDriver INSTANCE;
    static {
        try {
            DriverManager.registerDriver( INSTANCE = new PhoenixDriver() );
        } catch (SQLException e) {
            throw new IllegalStateException("Untable to register " + PhoenixDriver.class.getName() + ": "+ e.getMessage());
        }
    }
    private final ConcurrentMap<ConnectionInfo,ConnectionQueryServices> connectionQueryServicesMap = new ConcurrentHashMap<ConnectionInfo,ConnectionQueryServices>(3);

    public PhoenixDriver() { // for Squirrel
        // Use production services implementation
        super(new QueryServicesImpl(withDefaults(HBaseConfiguration.create())));
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Accept the url only if test=true attribute not set
        return super.acceptsURL(url) && !(url.endsWith(";test=true") || url.contains(";test=true;"));
    }

    @Override
    protected ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
        ConnectionInfo connInfo = getConnectionInfo(url);
        String zookeeperQuorum = connInfo.getZookeeperQuorum();
        Integer port = connInfo.getPort();
        String rootNode = connInfo.getRootNode();
        boolean isConnectionless = false;
        // Normalize connInfo so that a url explicitly specifying versus implicitly inheriting
        // the default values will both share the same ConnectionQueryServices.
        Configuration globalConfig = getQueryServices().getConfig();
        if (zookeeperQuorum == null) {
            zookeeperQuorum = globalConfig.get(ZOOKEEPER_QUARUM_ATTRIB);
            if (zookeeperQuorum == null) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                .setMessage(url).build().buildException();
            }
        }
        isConnectionless = PhoenixRuntime.CONNECTIONLESS.equals(zookeeperQuorum);

        if (port == null) {
            if (!isConnectionless) {
                String portStr = globalConfig.get(ZOOKEEPER_PORT_ATTRIB);
                if (portStr != null) {
                    try {
                        port = Integer.parseInt(portStr);
                    } catch (NumberFormatException e) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                        .setMessage(url).build().buildException();
                    }
                }
            }
        } else if (isConnectionless) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
            .setMessage("Port may not be specified when using the connectionless url \"" + url + "\"").build().buildException();
        }
        if (rootNode == null) {
            if (!isConnectionless) {
                rootNode = globalConfig.get(ZOOKEEPER_ROOT_NODE_ATTRIB);
            }
        } else if (isConnectionless) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
            .setMessage("Root node may not be specified when using the connectionless url \"" + url + "\"").build().buildException();
        }
        ConnectionInfo normalizedConnInfo = new ConnectionInfo(zookeeperQuorum, port, rootNode);
        ConnectionQueryServices connectionQueryServices = connectionQueryServicesMap.get(normalizedConnInfo);
        if (connectionQueryServices == null) {
            if (isConnectionless) {
                connectionQueryServices = new ConnectionlessQueryServicesImpl(getQueryServices());
            } else {
                Configuration childConfig = HBaseConfiguration.create(globalConfig);
                if (connInfo.getZookeeperQuorum() != null) {
                    childConfig.set(ZOOKEEPER_QUARUM_ATTRIB, connInfo.getZookeeperQuorum());
                } else if (childConfig.get(ZOOKEEPER_QUARUM_ATTRIB) == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
                    .setMessage(url).build().buildException();
                }
                if (connInfo.getPort() != null) {
                	childConfig.setInt(ZOOKEEPER_PORT_ATTRIB, connInfo.getPort());
                }
                if (connInfo.getRootNode() != null) {
                    childConfig.set(ZOOKEEPER_ROOT_NODE_ATTRIB, connInfo.getRootNode());
                }
                connectionQueryServices = new ConnectionQueryServicesImpl(getQueryServices(), childConfig);
            }
            connectionQueryServices.init(url, info);
            ConnectionQueryServices prevValue = connectionQueryServicesMap.putIfAbsent(normalizedConnInfo, connectionQueryServices);
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
