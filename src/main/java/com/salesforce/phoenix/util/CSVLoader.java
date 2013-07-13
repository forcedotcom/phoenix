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

import java.io.FileReader;
import java.sql.*;
import java.util.*;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.PDataType;

/***
 * Upserts CSV data using Phoenix JDBC connection
 * 
 * @author mchohan
 * 
 */
public class CSVLoader {

	private final PhoenixConnection conn;
	private final String tableName;
    private final List<String> columns;
    private final boolean isStrict;
    
    private int unfoundColumnCount;

	public CSVLoader(PhoenixConnection conn, String tableName, List<String> columns, boolean isStrict) {
		this.conn = conn;
		this.tableName = tableName;
		this.columns = columns;
		this.isStrict = isStrict;
	}

	/**
	 * Upserts data from CSV file. Data is batched up based on connection batch
	 * size. Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. Note: Column Names are
	 * expected as first line of CSV file.
	 * 
	 * @param fileName
	 * @throws Exception
	 */
	public void upsert(String fileName) throws Exception {
		CSVReader reader = new CSVReader(new FileReader(fileName));
		upsert(reader);
	}

	/**
	 * Upserts data from CSV file. Data is batched up based on connection batch
	 * size. Column PDataType is read from metadata and is used to convert
	 * column value to correct type before upsert. Note: Column Names are
	 * expected as first line of CSV file.
	 * 
	 * @param reader CSVReader instance
	 * @throws Exception
	 */
	public void upsert(CSVReader reader) throws Exception {
	    List<String> columns = this.columns;
	    if (columns != null && columns.isEmpty()) {
	        columns = Arrays.asList(reader.readNext());
	    }
		ColumnInfo[] columnInfo = generateColumnInfo(columns);
        PreparedStatement stmt = null;
        PreparedStatement[] stmtCache = null;
		if (columns == null) {
		    stmtCache = new PreparedStatement[columnInfo.length];
		} else {
		    String upsertStatement = QueryUtil.constructUpsertStatement(columnInfo, tableName, columnInfo.length - unfoundColumnCount);
		    stmt = conn.prepareStatement(upsertStatement);
		}
		String[] nextLine;
		int rowCount = 0;
		int upsertBatchSize = conn.getMutateBatchSize();
		Object upsertValue = null;
		long start = System.currentTimeMillis();

		// Upsert data based on SqlType of each column
		while ((nextLine = reader.readNext()) != null) {
		    if (columns == null) {
		        stmt = stmtCache[nextLine.length-1];
		        if (stmt == null) {
	                String upsertStatement = QueryUtil.constructUpsertStatement(columnInfo, tableName, nextLine.length);
	                stmt = conn.prepareStatement(upsertStatement);
	                stmtCache[nextLine.length-1] = stmt;
		        }
		    }
			for (int index = 0; index < columnInfo.length; index++) {
			    if (columnInfo[index] == null) {
			        continue;
			    }
				upsertValue = convertTypeSpecificValue(nextLine[index], columnInfo[index].getSqlType());
				if (upsertValue != null) {
					stmt.setObject(index + 1, upsertValue, columnInfo[index].getSqlType());
				} else {
					stmt.setNull(index + 1, columnInfo[index].getSqlType());
				}
			}
			stmt.execute();

			// Commit when batch size is reached
			if (++rowCount % upsertBatchSize == 0) {
				conn.commit();
				System.out.println("Rows upserted: " + rowCount);
			}
		}
		conn.commit();
		double elapsedDuration = ((System.currentTimeMillis() - start) / 1000.0);
		System.out.println("CSV Upsert complete. " + rowCount + " rows upserted");
		System.out.println("Time: " + elapsedDuration + " sec(s)\n");
	}
	
	/**
	 * Gets CSV string input converted to correct type 
	 */
	private Object convertTypeSpecificValue(String s, Integer sqlType) throws Exception {
	    return PDataType.fromSqlType(sqlType).toObject(s);
	}

	/**
	 * Get array of ColumnInfos that contain Column Name and its associated
	 * PDataType
	 * 
	 * @param columns
	 * @return
	 * @throws SQLException
	 */
	private ColumnInfo[] generateColumnInfo(List<String> columns)
			throws SQLException {
	    Map<String,Integer> columnNameToTypeMap = Maps.newLinkedHashMap();
        DatabaseMetaData dbmd = conn.getMetaData();
        // TODO: escape wildcard characters here because we don't want that behavior here
        String escapedTableName = StringUtil.escapeLike(tableName);
        String[] schemaAndTable = escapedTableName.split("\\.");
        ResultSet rs = dbmd.getColumns(null, (schemaAndTable.length == 1 ? "" : schemaAndTable[0]),
                        (schemaAndTable.length == 1 ? escapedTableName : schemaAndTable[1]),
                        null);
        while (rs.next()) {
            columnNameToTypeMap.put(rs.getString(QueryUtil.COLUMN_NAME_POSITION), rs.getInt(QueryUtil.DATA_TYPE_POSITION));
        }
        ColumnInfo[] columnType;
	    if (columns == null) {
            int i = 0;
            columnType = new ColumnInfo[columnNameToTypeMap.size()];
            for (Map.Entry<String, Integer> entry : columnNameToTypeMap.entrySet()) {
                columnType[i++] = new ColumnInfo(entry.getKey(),entry.getValue());
            }
	    } else {
            // Leave "null" as indication to skip b/c it doesn't exist
            columnType = new ColumnInfo[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                String columnName = SchemaUtil.normalizeIdentifier(columns.get(i).trim());
                Integer sqlType = columnNameToTypeMap.get(columnName);
                if (sqlType == null) {
                    if (isStrict) {
                        throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                            .setColumnName(columnName).setTableName(tableName).build().buildException();
                    }
                    unfoundColumnCount++;
                } else {
                    columnType[i] = new ColumnInfo(columnName, sqlType);
                }
            }
            if (unfoundColumnCount == columns.size()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                    .setColumnName(Arrays.toString(columns.toArray(new String[0]))).setTableName(tableName).build().buildException();
            }
	    }
		return columnType;
	}
}
