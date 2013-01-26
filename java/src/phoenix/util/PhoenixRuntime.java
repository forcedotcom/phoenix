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
package phoenix.util;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.client.Mutation;

import phoenix.jdbc.PhoenixConnection;
import phoenix.jdbc.PhoenixProdEmbeddedDriver;

/**
 * 
 * Collection of non JDBC compliant utility methods
 *
 * @author jtaylor
 * @since 0.1
 */
public class PhoenixRuntime {
    /**
     * Use this connection property to control HBase timestamps
     * by specifying your own long timestamp value at connection time. All
     * queries will use this as the upper bound of the time range for scans
     * and DDL, and DML will use this as t he timestamp for key values.
     */
    public static final String CURRENT_SCN_ATTRIB = "CurrentSCN";
    /**
     * Root for the JDBC URL that the Phoenix accepts accepts.
     */
    public final static String EMBEDDED_JDBC_PROTOCOL = "jdbc:phoenix:";
    
    /**
     * Use this connection property to control the number of rows that are
     * batched together on an UPSERT INTO table1... SELECT ... FROM table2.
     * It's only used when autoCommit is true and your source table is
     * different than your target table or your SELECT statement has a 
     * GROUP BY clause.
     */
    public final static String UPSERT_BATCH_SIZE_ATTRIB = "UpsertBatchSize";
    
    /**
     * Use this connection property to help with fairness of resource allocation
     * for the client and server. The value of the attribute determines the
     * bucket used to rollup resource usage for a particular tenant/organization. Each tenant
     * may only use a percentage of total resources, governed by the {@link phoenix.query.QueryServices}
     * configuration properties
     */
    public static final String TENANT_ID_ATTRIB = "TenantId";

    /**
     * Provides a mechanism to run SQL scripts against, where the arguments are:
     * 1) connection URL string
     * 2) one or more paths to SQL scripts
     * If a CurrentSCN property is set on the connection URL, then it is incremented
     * between processing, with each file being processed by a new connection at the
     * increment timestamp value.
     */
    public static void main(String [] args) {
        if (args.length < 2) {
            System.err.println("Usage: psql <connection-url> <path-to-sql-script>...");
            return;
        }
        
        try {
            Properties props = new Properties();
            Class.forName(PhoenixProdEmbeddedDriver.class.getName());
            PhoenixConnection conn = DriverManager.getConnection(args[0]).unwrap(PhoenixConnection.class);

            for (int i = 1; i < args.length; i++) {
           		PhoenixRuntime.executeStatements(conn, new FileReader(args[i]), Collections.emptyList());
                Long scn = conn.getSCN();
                // If specifying SCN, increment it between processing files to allow
                // for later files to see earlier files tables.
                if (scn != null) {
                    scn++;
                    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn.toString());
                    conn.close();
                    conn = DriverManager.getConnection(args[0], props).unwrap(PhoenixConnection.class);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private PhoenixRuntime() {
    }
    
    /**
     * Runs a series of semicolon-terminated SQL statements using the connection provided, returning
     * the number of SQL statements executed. Note that if the connection has specified an SCN through
     * the {@link phoenix.util.PhoenixRuntime#CURRENT_SCN_ATTRIB} connection property, then the timestamp
     * is bumped up by one after each statement execution.
     * @param conn an open JDBC connection
     * @param reader a reader for semicolumn separated SQL statements
     * @param binds the binds for all statements
     * @return the number of SQL statements that were executed
     * @throws IOException
     * @throws SQLException
     */
    public static int executeStatements(Connection conn, Reader reader, List<Object> binds) throws IOException,SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        // Turn auto commit to true when running scripts in case there's DML
        pconn.setAutoCommit(true);
        return pconn.executeStatements(reader, binds, System.out);
    }
    
    /**
     * Get the list of uncommitted HBase mutations for the connection. Currently used to write an
     * Phoenix-compliant HFile from a map/reduce job.
     * @param conn an open JDBC connection
     * @return the list of HBase mutations for uncommitted data
     * @throws SQLException 
     */
    public static List<Mutation> getUncommittedMutations(Connection conn) throws SQLException {
        return conn.unwrap(PhoenixConnection.class).getMutationState().toMutations();
    }
}
