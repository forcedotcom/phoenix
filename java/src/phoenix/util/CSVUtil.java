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

import java.io.FileReader;
import java.sql.*;

import phoenix.jdbc.PhoenixConnection;
import phoenix.jdbc.PhoenixProdEmbeddedDriver;
import phoenix.schema.PDataType;
import au.com.bytecode.opencsv.CSVReader;

/***
 * Upserts CSV data using Phoenix JDBC connection
 * 
 * @author mchohan
 * 
 */
public class CSVUtil {

	private PhoenixConnection conn;
	private String tableName;

	public CSVUtil(PhoenixConnection conn, String tableName) {
		this.conn = conn;
		this.tableName = tableName;
	}

	/**
	 * Main method for CSV Upsert. Usage: pcsv <connection-url> <tablename> <path-to-csv>
	 * @param args
	 */
    public static void main(String [] args) {
        if (args.length !=3) {
            System.err.println("Usage: pcsv <connection-url> <tablename> <path-to-csv>");
            return;
        }
        
        try {
            Class.forName(PhoenixProdEmbeddedDriver.class.getName());
            PhoenixConnection conn = DriverManager.getConnection(args[0]).unwrap(PhoenixConnection.class);
        	new CSVUtil(conn, args[1]).upsert(args[2]);

        } catch (Throwable t) {
            t.printStackTrace();
        }
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
		System.out.println("Starting Upsert.\nTable: " + tableName + "\nCSV: " + fileName);
		upsert(reader);
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
	public void upsert(CSVReader reader) throws Exception {
		ColumnInfo[] columns = generateColumnInfo(reader.readNext());
		String upsertStatement = constructUpsertStatement(columns);
		PreparedStatement stmt = conn.prepareStatement(upsertStatement);
		String[] nextLine;
		int rowCount = 0;
		int upsertBatchSize = conn.getUpsertBatchSize();
		Object upsertValue = null;

		// Upsert data based on SqlType of each column
		while ((nextLine = reader.readNext()) != null) {
			for (int index = 0; index < columns.length; index++) {
				upsertValue = convertTypeSpecificValue(nextLine[index], columns[index].pDataType);
				if (upsertValue != null) {
					stmt.setObject(index + 1, upsertValue, columns[index].getPDataType().getSqlType());
				} else {
					stmt.setNull(index + 1, columns[index].getPDataType().getSqlType());
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
		System.out.println("CSV Upsert complete. Total number of rows upserted: " + rowCount);
	}
	
	/**
	 * Gets CSV string input converted to correct type 
	 */
	private Object convertTypeSpecificValue(String s, PDataType type) throws Exception {
	    return type.toObject(s);
	}

	/**
	 * Get array of ColumnInfos that contain Column Name and its associated
	 * PDataType
	 * 
	 * @param columns
	 * @return
	 * @throws SQLException
	 */
	private ColumnInfo[] generateColumnInfo(String[] columns)
			throws SQLException {
		ColumnInfo[] columnType = new ColumnInfo[columns.length];
		for (int i = 0; i < columns.length; i++) {
			columnType[i] = new ColumnInfo(columns[i].trim(),
					getColumnPDataType(columns[i].trim()));
		}
		return columnType;
	}

	/**
	 * Get PDataType for a specific columnName
	 * 
	 * @param columnName
	 * @return
	 * @throws SQLException
	 */
	private PDataType getColumnPDataType(String columnName) throws SQLException {
		DatabaseMetaData dbmd = conn.getMetaData();
		String[] schemaAndTable = tableName.split("\\.");
		ResultSet rs = dbmd.getColumns(null, (schemaAndTable.length == 1 ? "" : schemaAndTable[0]),
						(schemaAndTable.length == 1 ? tableName : schemaAndTable[1]),
						columnName);
		rs.next();
		return PDataType.fromSqlType(rs.getInt("DATA_TYPE"));
	}

	/**
	 * ColumnInfo used to store Column Name and its associated PDataType
	 */
	private class ColumnInfo {
		private String columnName;
		private PDataType pDataType;

		public ColumnInfo(String columnName, PDataType pDataType) {
			this.columnName = columnName;
			this.pDataType = pDataType;
		}

		public String getColumnName() {
			return columnName;
		}

		public PDataType getPDataType() {
			return pDataType;
		}
	}

	/**
	 * Constructs upsert statement based on ColumnInfo array
	 * 
	 * @param columnTypes
	 * @return
	 */
	private String constructUpsertStatement(ColumnInfo[] columnTypes) {
		String upsertStatement = "UPSERT INTO ";
		upsertStatement += tableName;
		upsertStatement += '(';
		for (ColumnInfo columnType : columnTypes) {
			upsertStatement += columnType.getColumnName();
			upsertStatement += ',';
		}
		upsertStatement += ") VALUES (";
		for (int i = 0; i < columnTypes.length; i++) {
			upsertStatement += "?,";
		}
		upsertStatement += ')';
		upsertStatement = upsertStatement.replace(",)", ")");
		return upsertStatement;
	}
}
