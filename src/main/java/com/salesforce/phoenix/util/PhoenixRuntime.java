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
package com.salesforce.phoenix.util;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.jdbc.PhoenixDriver;

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
    public final static String JDBC_PROTOCOL = "jdbc:phoenix";
    public final static char JDBC_PROTOCOL_TERMINATOR = ';';
    public final static char JDBC_PROTOCOL_SEPARATOR = ':';
    
    @Deprecated
    public final static String EMBEDDED_JDBC_PROTOCOL = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    
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
     * may only use a percentage of total resources, governed by the {@link com.salesforce.phoenix.query.QueryServices}
     * configuration properties
     */
    public static final String TENANT_ID_ATTRIB = "TenantId";

    /**
     * Use this as the zookeeper quorum name to have a connection-less connection. This enables
     * Phoenix-compatible HFiles to be created in a map/reduce job by creating tables,
     * upserting data into them, and getting the uncommitted state through {@link #getUncommittedData(Connection)}
     */
    public final static String CONNECTIONLESS = "none";
    
    private static final String UPGRADE_OPTION = "-u";
    private static final String TABLE_OPTION = "-t";
    private static final String HEADER_OPTION = "-h";
    private static final String STRICT_OPTION = "-s";
    private static final String HEADER_IN_LINE = "in-line";
    private static final String SQL_FILE_EXT = ".sql";
    private static final String CSV_FILE_EXT = ".csv";
    
    private static void usageError() {
        System.err.println("Usage: psql [-t table-name] [-h comma-separated-column-names | in-line] <zookeeper>  <path-to-sql-or-csv-file>...\n" +
                "  By default, the name of the CSV file is used to determine the Phoenix table into which the CSV data is loaded\n" +
                "  and the ordinal value of the columns determines the mapping.\n" +
                "  -t overrides the table into which the CSV data is loaded\n" +
                "  -h overrides the column names to which the CSV data maps\n" +
                "     A special value of in-line indicating that the first line of the CSV file\n" +
                "     determines the column to which the data maps.\n" +
                "  -s uses strict mode by throwing an exception if a column name doesn't match during CSV loading.\n" +
                "Examples:\n" +
                "  psql localhost my_ddl.sql\n" +
                "  psql localhost my_ddl.sql my_table.csv\n" +
                "  psql my_cluster:1825 -t my_table my_table2012-Q3.csv\n" +
                "  psql my_cluster -t my_table -h col1,col2,col3 my_table2012-Q3.csv\n"
        );
        System.exit(-1);
    }
    /**
     * Provides a mechanism to run SQL scripts against, where the arguments are:
     * 1) connection URL string
     * 2) one or more paths to either SQL scripts or CSV files
     * If a CurrentSCN property is set on the connection URL, then it is incremented
     * between processing, with each file being processed by a new connection at the
     * increment timestamp value.
     */
    public static void main(String [] args) {
        if (args.length < 2) {
            usageError();
        }
        
        try {
            String tableName = null;
            List<String> columns = null;
            boolean isStrict = false;
            boolean isUpgrade = false;

            int i = 0;
            for (; i < args.length; i++) {
                if (TABLE_OPTION.equals(args[i])) {
                    if (++i == args.length || tableName != null) {
                        usageError();
                    }
                    tableName = args[i];
                } else if (HEADER_OPTION.equals(args[i])) {
                    if (++i >= args.length || columns != null) {
                        usageError();
                    }
                    String header = args[i];
                    if (HEADER_IN_LINE.equals(header)) {
                        columns = Collections.emptyList();
                    } else {
                        columns = Lists.newArrayList();
                        StringTokenizer tokenizer = new StringTokenizer(header,",");
                        while(tokenizer.hasMoreTokens()) {
                            columns.add(tokenizer.nextToken());
                        }
                    }
                } else if (STRICT_OPTION.equals(args[i])) {
                    isStrict = true;
                } else if (UPGRADE_OPTION.equals(args[i])) {
                    isUpgrade = true;
                } else {
                    break;
                }
            }
            if (i == args.length) {
                usageError();
            }
            
            Properties props = new Properties();
            if (isUpgrade) {
                props.setProperty(SchemaUtil.UPGRADE_TO_2_0, Integer.toString(SchemaUtil.SYSTEM_TABLE_NULLABLE_VAR_LENGTH_COLUMNS));
            }
            String connectionUrl = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + args[i++];
            PhoenixConnection conn = DriverManager.getConnection(connectionUrl, props).unwrap(PhoenixConnection.class);
            
            if (SchemaUtil.upgradeColumnCount(connectionUrl, props) > 0) {
                SchemaUtil.upgradeTo2(conn);
                return;
            }
            
            for (; i < args.length; i++) {
                String fileName = args[i];
                if (fileName.endsWith(SQL_FILE_EXT)) {
               		PhoenixRuntime.executeStatements(conn, new FileReader(args[i]), Collections.emptyList());
                } else if (fileName.endsWith(CSV_FILE_EXT)) {
                    if (tableName == null) {
                        tableName = fileName.substring(fileName.lastIndexOf(File.separatorChar) + 1, fileName.length()-CSV_FILE_EXT.length());
                    }
                    CSVLoader csvLoader = new CSVLoader(conn, tableName, columns, isStrict);
                    csvLoader.upsert(fileName);
                } else {
                    usageError();
                }
                Long scn = conn.getSCN();
                // If specifying SCN, increment it between processing files to allow
                // for later files to see earlier files tables.
                if (scn != null) {
                    scn++;
                    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn.toString());
                    conn.close();
                    conn = DriverManager.getConnection(connectionUrl, props).unwrap(PhoenixConnection.class);
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
     * the {@link com.salesforce.phoenix.util.PhoenixRuntime#CURRENT_SCN_ATTRIB} connection property, then the timestamp
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
     * Get the list of uncommitted KeyValues for the connection. Currently used to write an
     * Phoenix-compliant HFile from a map/reduce job.
     * @param conn an open JDBC connection
     * @return the list of HBase mutations for uncommitted data
     * @throws SQLException 
     */
    @Deprecated
    public static List<KeyValue> getUncommittedData(Connection conn) throws SQLException {
        Iterator<Pair<byte[],List<KeyValue>>> iterator = getUncommittedDataIterator(conn);
        if (iterator.hasNext()) {
            return iterator.next().getSecond();
        }
        return Collections.emptyList();
    }
    
    /**
     * Get the list of uncommitted KeyValues for the connection. Currently used to write an
     * Phoenix-compliant HFile from a map/reduce job.
     * @param conn an open JDBC connection
     * @return the list of HBase mutations for uncommitted data
     * @throws SQLException 
     */
    public static Iterator<Pair<byte[],List<KeyValue>>> getUncommittedDataIterator(Connection conn) throws SQLException {
        final Iterator<Pair<byte[],List<Mutation>>> iterator = conn.unwrap(PhoenixConnection.class).getMutationState().toMutations();
        return new Iterator<Pair<byte[],List<KeyValue>>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Pair<byte[], List<KeyValue>> next() {
                Pair<byte[],List<Mutation>> pair = iterator.next();
                List<KeyValue> keyValues = Lists.newArrayListWithExpectedSize(pair.getSecond().size() * 5); // Guess-timate 5 key values per row
                for (Mutation mutation : pair.getSecond()) {
                    for (List<KeyValue> keyValueList : mutation.getFamilyMap().values()) {
                        for (KeyValue keyValue : keyValueList) {
                            keyValues.add(keyValue);
                        }
                    }
                }
                Collections.sort(keyValues, KeyValue.COMPARATOR);
                return new Pair<byte[], List<KeyValue>>(pair.getFirst(),keyValues);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
            
        };
    }
}