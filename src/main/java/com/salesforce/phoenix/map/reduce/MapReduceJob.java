/*******************************************************************************
* Copyright (c) 2013, Salesforce.com, Inc.
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
* Redistributions of source code must retain the above copyright notice,
* this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
* this list of conditions and the following disclaimer in the documentation
* and/or other materials provided with the distribution.
* Neither the name of Salesforce.com nor the names of its contributors may
* be used to endorse or promote products derived from this software without
* specific prior written permission.
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
package com.salesforce.phoenix.map.reduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import au.com.bytecode.opencsv.CSVReader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.QueryUtil;

public class MapReduceJob {

	public static class PhoenixMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{
		
		private Connection conn_none 	= null;
		private Connection conn_zk 	= null;
		private PreparedStatement[] stmtCache;
		private String tableName;
		private String schemaName;
		private String[] createDDL = new String[2];
		Map<Integer, Integer> colDetails = new LinkedHashMap<Integer, Integer>();
		boolean ignoreUpsertError = true;
		private String zookeeperIP;
		
		/**
		 * Get the phoenix jdbc connection.
		 */
		
		private static String getUrl(String url) {
	        	return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + url;
	  	}
		
		/***
		 * Get the column information from the table metaData.
		 * Cretae a map of col-index and col-data-type.
		 * Create the upsert Prepared Statement based on the map-size.
		 */
		
		@Override
		public void setup(Context context) throws InterruptedException{
			Properties props = new Properties();
			
			try {
				zookeeperIP 		= context.getConfiguration().get("zk");
				
				//Connectionless mode used for upsert and iterate over KeyValue pairs
				conn_none			= DriverManager.getConnection(getUrl(PhoenixRuntime.CONNECTIONLESS), props);
				//ZK connection used to get the table meta-data
				conn_zk				= DriverManager.getConnection(getUrl(zookeeperIP), props);
				
				schemaName			= context.getConfiguration().get("schemaName");
				tableName 			= context.getConfiguration().get("tableName");
				createDDL[0]		= context.getConfiguration().get("createTableSQL");
				createDDL[1]		= context.getConfiguration().get("createIndexSQL");
				ignoreUpsertError 	= context.getConfiguration().get("IGNORE.INVALID.ROW").equalsIgnoreCase("0") ? false : true;
				
				for(String s : createDDL){
					if(s == null || s.trim().length() == 0)
						continue;

					try {
						PreparedStatement prepStmt = conn_none.prepareStatement(s);
						prepStmt.execute();
					} catch (SQLException e) {
						System.err.println("Error creating the table in connectionless mode :: " + e.getMessage());
					}
				}
				
				//Get the resultset from the actual zookeeper connection. Connectionless mode throws "UnSupportedOperation" exception for this
				ResultSet rs 		= conn_zk.getMetaData().getColumns(null, schemaName, tableName, null);
				//This map holds the key-value pair of col-position and its data type
				int i = 1;
				while(rs.next()){
					colDetails.put(i, rs.getInt(QueryUtil.DATA_TYPE_POSITION));
					i++;
				}
				
				stmtCache = new PreparedStatement[colDetails.size()];
				ArrayList<String> cols = new ArrayList<String>();
				for(i = 0 ; i < colDetails.size() ; i++){
					cols.add("?");
					String prepValues = StringUtils.join(cols, ",");
					String upsertStmt = "upsert into " + schemaName + "." + tableName + " values (" + prepValues + ")";
					try {
						stmtCache[i] = conn_none.prepareStatement(upsertStmt);
					} catch (SQLException e) {
						System.err.println("Error preparing the upsert statement" + e.getMessage());
						if(!ignoreUpsertError){
							throw (new InterruptedException(e.getMessage()));
						}
					}
				}
			} catch (SQLException e) {
					System.err.println("Error occurred in connecting to Phoenix HBase" + e.getMessage());
			}
			
	  	}
		
		/* Tokenize the text input line based on the "," delimeter.
		*  TypeCast the token based on the col-data-type using the convertTypeSpecificValue API below.
		*  Upsert the data. DO NOT COMMIT.
		*  Use Phoenix's getUncommittedDataIterator API to parse the uncommited data to KeyValue pairs.
		*  Emit the row-key and KeyValue pairs from Mapper to allow sorting based on row-key.
		*  Finally, do connection.rollback( to preserve table state).
		*/
		
		@Override
		public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException{
			
            CSVReader reader = new CSVReader(new InputStreamReader(new ByteArrayInputStream(line.toString().getBytes())), ',');
			try {
				String[] tokens = reader.readNext();
				
				PreparedStatement upsertStatement;
				if(tokens.length >= stmtCache.length){
					//If CVS values are more than the number of cols in the table, apply the col count cap
					upsertStatement = stmtCache[stmtCache.length - 1];
				}else{
					//Else, take the corresponding upsertStmt from cached array 
					upsertStatement = stmtCache[tokens.length - 1];
				}

				for(int i = 0 ; i < tokens.length && i < colDetails.size() ;i++){
					upsertStatement.setObject(i+1, convertTypeSpecificValue(tokens[i], colDetails.get(new Integer(i+1))));
				}
				
				upsertStatement.execute();
			} catch (SQLException e) {
				System.err.println("Failed to upsert data in the Phoenix :: " + e.getMessage());
				if(!ignoreUpsertError){
					throw (new InterruptedException(e.getMessage()));
				}
			} catch (Exception e) {
				System.err.println("Failed to upsert data in the Phoenix :: " + e.getMessage());
			}
			
			Iterator<Pair<byte[],List<KeyValue>>> dataIterator = null;
			try {
				dataIterator = PhoenixRuntime.getUncommittedDataIterator(conn_none);
			} catch (SQLException e) {
				System.err.println("Failed to retrieve the data iterator for Phoenix table :: " + e.getMessage());
			} finally {
			    reader.close();
			}
			
			while(dataIterator != null && dataIterator.hasNext()){
				Pair<byte[],List<KeyValue>> row = dataIterator.next();
				for(KeyValue kv : row.getSecond()){
					context.write(new ImmutableBytesWritable(kv.getKey()), kv);
				}
			}
			
			try {
				conn_none.rollback();
			} catch (SQLException e) {
				System.err.println("Transaction rollback failed.");
			}
		}
		
		/*
		* Do connection.close()
		*/
		
		@Override
		public void cleanup(Context context) {
	  		try {
	  			conn_zk.close();
				conn_none.close();
			} catch (SQLException e) {
				System.err.println("Failed to close the JDBC connection");
			}
	  	}
		
		private Object convertTypeSpecificValue(String s, Integer sqlType) throws Exception {
			return PDataType.fromSqlType(sqlType).toObject(s);
		}
	}
	
}
