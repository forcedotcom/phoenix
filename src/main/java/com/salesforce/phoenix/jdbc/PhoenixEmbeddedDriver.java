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

import java.sql.*;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.SQLCloseable;



/**
 * 
 * Abstract base class for JDBC Driver implementation of Phoenix
 * 
 * @author jtaylor
 * @since 0.1
 */
public abstract class PhoenixEmbeddedDriver implements Driver, com.salesforce.phoenix.jdbc.Jdbc7Shim.Driver, SQLCloseable {
    /**
     * The protocol for Phoenix Network Client 
     */ 
    private final static String DNC_JDBC_PROTOCOL_SUFFIX = "//";
    private static final String TERMINATOR = "" + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR;
    private static final String DELIMITERS = TERMINATOR + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

    private final static DriverPropertyInfo[] EMPTY_INFO = new DriverPropertyInfo[0];
    public final static String MAJOR_VERSION_PROP = "DriverMajorVersion";
    public final static String MINOR_VERSION_PROP = "DriverMinorVersion";
    public final static String DRIVER_NAME_PROP = "DriverName";
    private final static int MAJOR_VERSION = 1;
    private final static int MINOR_VERSION = 0;
    
    private final QueryServices services;

    
    PhoenixEmbeddedDriver(QueryServices queryServices) {
        services = queryServices;
    }
    
    private String getDriverName() {
        return this.getClass().getName();
    }
    
    public QueryServices getQueryServices() {
        return services;
    }
     
    protected static ConnectionInfo getConnectionInfo(String url) throws SQLException {
        StringTokenizer tokenizer = new StringTokenizer(url.substring(PhoenixRuntime.JDBC_PROTOCOL.length()),DELIMITERS, true);
        int i = 0;
        boolean isMalformedUrl = false;
        String[] tokens = new String[3];
        String token = null;
        while (tokenizer.hasMoreTokens() && !(token=tokenizer.nextToken()).equals(TERMINATOR) && tokenizer.hasMoreTokens() && i < tokens.length) {
            token = tokenizer.nextToken();
            // This would mean we have an empty string for a token which is illegal
            if (DELIMITERS.contains(token)) {
                isMalformedUrl = true;
                break;
            }
            tokens[i++] = token;
        }
        Integer port = null;
        if (!isMalformedUrl) {
            if (tokenizer.hasMoreTokens() && !TERMINATOR.equals(token)) {
                isMalformedUrl = true;
            } else if (i > 1) {
                try {
                    port = Integer.parseInt(tokens[1]);
                    isMalformedUrl = port < 0;
                } catch (NumberFormatException e) {
                    // If we have 3 tokens, then the second one must be a port.
                    // If we only have 2 tokens, the second one might be the root node:
                    // Assume that is the case if we get a NumberFormatException
                    if (! (isMalformedUrl = i == 3) ) {
                        tokens[2] = tokens[1];
                    }
                    
                }
            }
        }
        if (isMalformedUrl) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.MALFORMED_CONNECTION_URL)
            .setMessage(url).build().buildException();
        }
        return new ConnectionInfo(tokens[0],port,tokens[2]);
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            // A connection string of "jdbc:phoenix" is supported, since
            // all the connection information can potentially be gotten
            // out of the HBase config file
            if (url.length() == PhoenixRuntime.JDBC_PROTOCOL.length()) {
                return true;
            }
            // Same as above, except for "jdbc:phoenix;prop=<value>..."
            if (PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR == url.charAt(PhoenixRuntime.JDBC_PROTOCOL.length())) {
                return true;
            }
            if (PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR == url.charAt(PhoenixRuntime.JDBC_PROTOCOL.length())) {
                int protoLength = PhoenixRuntime.JDBC_PROTOCOL.length() + 1;
                // A connection string of "jdbc:phoenix:" matches this driver,
                // but will end up as a MALFORMED_CONNECTION_URL exception later.
                if (url.length() == protoLength) {
                    return true;
                }
                // A connection string of the form "jdbc:phoenix://" means that
                // the driver is remote which isn't supported, so return false.
                if (!url.startsWith(DNC_JDBC_PROTOCOL_SUFFIX, protoLength)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        ConnectionQueryServices connectionServices = getConnectionQueryServices(url, info);
        info.setProperty(MAJOR_VERSION_PROP, Integer.toString(getMajorVersion()));
        info.setProperty(MINOR_VERSION_PROP, Integer.toString(getMinorVersion()));
        info.setProperty(DRIVER_NAME_PROP, getDriverName());
        PhoenixConnection connection = connectionServices.connect(url, info);
        return connection;
    }

    /**
     * Get or create if necessary a QueryServices that is associated with the HBase zookeeper quorum
     * name (part of the connection URL). This will cause the underlying Configuration held by the
     * QueryServices to be shared for all connections to the same HBase cluster.
     * @param url connection URL
     * @param info connection properties
     * @return new or cached QuerySerices used to establish a new Connection.
     * @throws SQLException
     */
    protected abstract ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException;
    
    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return EMPTY_INFO;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public void close() throws SQLException {
    }
    
    /**
     * 
     * Class to encapsulate connection info for HBase
     *
     * @author jtaylor
     * @since 0.1.1
     */
    protected static class ConnectionInfo {
        private final Integer port;
        private final String rootNode;
        private final String zookeeperQuorum;
        
        protected ConnectionInfo(String zookeeperQuorum, Integer port, String rootNode) {
            this.zookeeperQuorum = zookeeperQuorum;
            this.port = port;
            this.rootNode = rootNode;
        }

        public String getZookeeperQuorum() {
            return zookeeperQuorum;
        }

        public Integer getPort() {
            return port;
        }

        public String getRootNode() {
            return rootNode;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((zookeeperQuorum == null) ? 0 : zookeeperQuorum.hashCode());
            result = prime * result + ((port == null) ? 0 : port.hashCode());
            result = prime * result + ((rootNode == null) ? 0 : rootNode.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            ConnectionInfo other = (ConnectionInfo)obj;
            if (zookeeperQuorum == null) {
                if (other.zookeeperQuorum != null) return false;
            } else if (!zookeeperQuorum.equals(other.zookeeperQuorum)) return false;
            if (port == null) {
                if (other.port != null) return false;
            } else if (!port.equals(other.port)) return false;
            if (rootNode == null) {
                if (other.rootNode != null) return false;
            } else if (!rootNode.equals(other.rootNode)) return false;
            return true;
        }
        
        @Override
        public String toString() {
            return zookeeperQuorum + (port == null ? "" : ":" + port) + (rootNode == null ? "" : ":" + rootNode);
        }
    }
}
