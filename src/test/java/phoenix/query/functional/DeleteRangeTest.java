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
package phoenix.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static phoenix.util.TestUtil.PHOENIX_JDBC_URL;

import java.sql.*;

import org.junit.Test;

public class DeleteRangeTest extends BaseHBaseManagedTimeTest {
    private static final int NUMBER_OF_ROWS = 20;
    private static final int NTH_ROW_NULL = 5;
    
    private static void initTableValues(Connection conn) throws SQLException {
        ensureTableCreated(getUrl(),"IntIntKeyTest");
        String upsertStmt = "UPSERT INTO IntIntKeyTest VALUES(?,?)";
        PreparedStatement stmt = conn.prepareStatement(upsertStmt);
        for (int i = 0; i < NUMBER_OF_ROWS; i++) {
            stmt.setInt(1, i);
            if (i % NTH_ROW_NULL != 0) {
                stmt.setInt(2, i * 10);
            } else {
                stmt.setNull(2, Types.INTEGER);
            }
            stmt.execute();
        }
        conn.commit();
    }
    
    private void testDeleteRange(boolean autoCommit) throws Exception {
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL);
        initTableValues(conn);
        
        ResultSet rs;
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM IntIntKeyTest");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS, rs.getInt(1));

        rs = conn.createStatement().executeQuery("SELECT i FROM IntIntKeyTest WHERE j IS NULL");
        int i = 0, count = 0;
        while (rs.next()) {
            assertEquals(i,rs.getInt(1));
            i += NTH_ROW_NULL;
            count++;
        }
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM IntIntKeyTest WHERE j IS NOT NULL");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS-count, rs.getInt(1));

        conn.setAutoCommit(autoCommit);
        String deleteStmt = "DELETE FROM IntIntKeyTest WHERE i >= ? and i < ?";
        PreparedStatement stmt = conn.prepareStatement(deleteStmt);
        stmt.setInt(1, 5);
        stmt.setInt(2, 10);
        stmt.execute();
        if (!autoCommit) {
            conn.commit();
        }
        
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM IntIntKeyTest");
        assertTrue(rs.next());
        assertEquals(NUMBER_OF_ROWS - (10-5), rs.getInt(1));
    }
    
    @Test
    public void testDeleteRangeNoAutoCommit() throws Exception {
        testDeleteRange(false);
    }
    
    @Test
    public void testDeleteRangeAutoCommit() throws Exception {
        testDeleteRange(true);
    }
}
