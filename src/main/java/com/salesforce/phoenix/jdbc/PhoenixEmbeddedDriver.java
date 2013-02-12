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
    private final static String DNC_PROTOCOL = PhoenixRuntime.EMBEDDED_JDBC_PROTOCOL + "//";
    public final static String CONNECTIONLESS = "none";
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
            
    
    protected static String getZookeeperQuorum(String url) throws SQLException {
        int endIndex = url.indexOf(';');
        endIndex = endIndex == -1 ? url.length() : endIndex;
        // Search for next semicolon, not colon, so we pickup the entire URL(s) including the port (W-1443268)
        // TODO: test case for this
        int begIndex = PhoenixRuntime.EMBEDDED_JDBC_PROTOCOL.length();
        if (endIndex > begIndex) {
            return url.substring(begIndex, endIndex);
        }
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CONNECT_TO_ZOOKEEPER)
            .setMessage("connection url: " + url).build().buildException();
    }
    
    protected static String getZookeeperPort(String server) throws SQLException {
        int startIndex = server.lastIndexOf(':');
        if (startIndex == -1) {
        	return null;
        }
        return server.substring(startIndex + 1, server.length());
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return !url.startsWith(DNC_PROTOCOL) && url.startsWith(PhoenixRuntime.EMBEDDED_JDBC_PROTOCOL);
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
}
