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

import java.sql.SQLException;
import java.util.Properties;

import phoenix.query.ConnectionQueryServices;
import phoenix.query.ConnectionlessQueryServicesImpl;
import phoenix.query.QueryServices;
import phoenix.query.functional.*;


/**
 * 
 * JDBC Driver implementation of Phoenix for testing.
 * To use this driver, specify test=true in url.
 * 
 * @author jtaylor
 * @since 0.1
 */
public class PhoenixTestEmbeddedDriver extends PhoenixEmbeddedDriver {
    private final String url;
    private final ConnectionQueryServices queryServices;
    
    public PhoenixTestEmbeddedDriver(QueryServices services, String url, Properties props) throws SQLException {
        super(services);
        this.url = url;
        String serverName = getZookeeperQuorum(url);
        if (CONNECTIONLESS.equals(serverName)) {
            queryServices =  new ConnectionlessQueryServicesImpl(services);
        } else {
            queryServices =  new ConnectionQueryServicesTestImpl(services, services.getConfig());
        }
        queryServices.init(url, props);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        // Accept the url only if test=true attribute set
        return super.acceptsURL(url) && url.contains(";test=true");
    }

    @Override // public for testing
    public ConnectionQueryServices getConnectionQueryServices(String url, Properties info) throws SQLException {
        // Tests must connect to only one server, the same one that was specified at init time
        assert(this.url.equals(url));
        return queryServices;
    }
    
    @Override
    public void close() throws SQLException {
        queryServices.close();
    }
}
