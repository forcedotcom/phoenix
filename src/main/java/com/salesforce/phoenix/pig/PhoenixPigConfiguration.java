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

package com.salesforce.phoenix.pig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.util.ColumnInfo;
import com.salesforce.phoenix.util.QueryUtil;

/**
 * A container for configuration to be used with {@link PhoenixHBaseStorage}
 * 
 * @author pkommireddi
 * 
 */
public class PhoenixPigConfiguration {
	
	private static final Log LOG = LogFactory.getLog(PhoenixPigConfiguration.class);
	
	/**
	 * Speculative execution of Map tasks
	 */
	public static final String MAP_SPECULATIVE_EXEC = "mapred.map.tasks.speculative.execution";

	/**
	 * Speculative execution of Reduce tasks
	 */
	public static final String REDUCE_SPECULATIVE_EXEC = "mapred.reduce.tasks.speculative.execution";
	
	public static final String SERVER_NAME = "phoenix.hbase.server.name";
	
	public static final String TABLE_NAME = "phoenix.hbase.table.name";
	
	public static final String UPSERT_STATEMENT = "phoenix.upsert.stmt";
	
	public static final String UPSERT_BATCH_SIZE = "phoenix.upsert.batch.size";
	
	public static final long DEFAULT_UPSERT_BATCH_SIZE = 1000;
	
	private final Configuration conf;
	
	private Connection conn;
	private List<ColumnInfo> columnMetadataList;
		
	public PhoenixPigConfiguration(Configuration conf) {
		this.conf = conf;
	}
	
	public void configure(String server, String tableName, long batchSize) {
		conf.set(SERVER_NAME, server);
		conf.set(TABLE_NAME, tableName);
		conf.setLong(UPSERT_BATCH_SIZE, batchSize);
		conf.setBoolean(MAP_SPECULATIVE_EXEC, false);
		conf.setBoolean(REDUCE_SPECULATIVE_EXEC, false);
	}
	
	/**
	 * Creates a {@link Connection} and autoCommit to false.
	 * 
	 * @return 
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Properties props = new Properties();
		conn = DriverManager.getConnection(QueryUtil.getUrl(this.conf.get(SERVER_NAME)), props).unwrap(PhoenixConnection.class);
		conn.setAutoCommit(false);
		
		setup(conn);
		
		return conn;
	}
	
	/**
	 * This method creates the Upsert statement and the Column Metadata
	 * for the Pig query using {@link PhoenixHBaseStorage}. It also 
	 * determines the batch size based on user provided options.
	 * 
	 * @param conn
	 * @throws SQLException
	 */
	public void setup(Connection conn) throws SQLException {
		// Reset batch size
		long batchSize = getBatchSize() <= 0 ? ((PhoenixConnection) conn).getMutateBatchSize() : getBatchSize();
		conf.setLong(UPSERT_BATCH_SIZE, batchSize);
		
		if (columnMetadataList == null) {
			columnMetadataList = new ArrayList<ColumnInfo>();
			String[] tableMetadata = getTableMetadata(getTableName());
			ResultSet rs = conn.getMetaData().getColumns(null, tableMetadata[0], tableMetadata[1], null);
			while (rs.next()) {
				columnMetadataList.add(new ColumnInfo(rs.getString(QueryUtil.COLUMN_NAME_POSITION), rs.getInt(QueryUtil.DATA_TYPE_POSITION)));
			}
		}
		
		// Generating UPSERT statement without column name information.
		String upsertStmt = QueryUtil.constructUpsertStatement(null, getTableName(), columnMetadataList.size());
		LOG.info("Phoenix Upsert Statement: " + upsertStmt);
		conf.set(UPSERT_STATEMENT, upsertStmt);
	}
	
	public String getUpsertStatement() {
		return conf.get(UPSERT_STATEMENT);
	}

	public long getBatchSize() {
		return conf.getLong(UPSERT_BATCH_SIZE, DEFAULT_UPSERT_BATCH_SIZE);
	}


	public String getServer() {
		return conf.get(SERVER_NAME);
	}

	public List<ColumnInfo> getColumnMetadataList() {
		return columnMetadataList;
	}
	
	public String getTableName() {
		return conf.get(TABLE_NAME);
	}

	private String[] getTableMetadata(String table) {
		String[] schemaAndTable = table.split("\\.");
		assert schemaAndTable.length >= 1;

		if (schemaAndTable.length == 1) {
			return new String[] { "", schemaAndTable[0] };
		}

		return new String[] { schemaAndTable[0], schemaAndTable[1] };
	}

	
	public Configuration getConfiguration() {
		return this.conf;
	}

}
