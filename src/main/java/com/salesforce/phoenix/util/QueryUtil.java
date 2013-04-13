package com.salesforce.phoenix.util;

import java.sql.DatabaseMetaData;

public class QueryUtil {
	
	/**
	 *  Column name index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 */
	public static final int COLUMN_NAME_POSITION = 4;
	/**
	 * Data type index within ResultSet resulting from {@link DatabaseMetaData#getColumns(String, String, String, String)}
	 */
	public static final int DATA_TYPE_POSITION = 5;

	/**
	 * Generates the upsert statement based on number of ColumnInfo. If
	 * ColumnInfo is unavailable, it produces a generic UPSERT query without
	 * columns information using number of columns.
	 * 
	 * @return Upsert Statement
	 */
	public static String constructUpsertStatement(ColumnInfo[] columnTypes,
			String tableName, int numColumns) {
		if(numColumns <= 0) {
			throw new RuntimeException("Number of columns in HBase table cannot be less than 1");
		}
		StringBuilder sb = new StringBuilder();
		sb.append("UPSERT INTO ");
		sb.append(tableName);
		if (columnTypes != null) {
			sb.append("(");
			for (ColumnInfo columnType : columnTypes) {
				if (columnType != null) {
					sb.append(columnType.getColumnName());
					sb.append(",");
				}
			}
			// Remove the trailing comma
			sb.setLength(sb.length() - 1);
			sb.append(") ");
		}
		sb.append("\n");
		sb.append("VALUES (");
		for (short i = 0; i < numColumns - 1; i++) {
			sb.append("?,");
		}
		sb.append("?)");

		return sb.toString();
	}

	public static String getUrl(String server) {
		return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + server;
	}
}
