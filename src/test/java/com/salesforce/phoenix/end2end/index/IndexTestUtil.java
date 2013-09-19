package com.salesforce.phoenix.end2end.index;

import static com.salesforce.phoenix.jdbc.PhoenixDatabaseMetaData.*;

import java.sql.*;

public class IndexTestUtil {

    // the normal db metadata interface is insufficient for all fields needed for an
    // index table test.
    private static final String SELECT_DATA_INDEX_ROW = "SELECT " + TABLE_CAT_NAME
            + " FROM "
            + TYPE_SCHEMA + ".\"" + TYPE_TABLE
            + "\" WHERE "
            + TENANT_ID + " IS NULL AND " + TABLE_SCHEM_NAME + "=? AND " + TABLE_NAME_NAME + "=? AND " + COLUMN_NAME + " IS NULL AND " + TABLE_CAT_NAME + "=?";
    
    public static ResultSet readDataTableIndexRow(Connection conn, String schemaName, String tableName, String indexName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SELECT_DATA_INDEX_ROW);
        stmt.setString(1, schemaName);
        stmt.setString(2, tableName);
        stmt.setString(3, indexName);
        return stmt.executeQuery();
    }
}
