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
package com.salesforce.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Test;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.query.BaseConnectionlessQueryTest;
import com.salesforce.phoenix.schema.ColumnNotFoundException;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.PTable.ViewType;
import com.salesforce.phoenix.schema.PTableType;
import com.salesforce.phoenix.util.TestUtil;

public class ViewCompileTest extends BaseConnectionlessQueryTest {
    @Test
    public void testViewTypeCalculation() throws Exception {
        assertViewType(new String[] {
            "CREATE VIEW v1 AS SELECT * FROM t WHERE k1 = 1 AND k2 = 'foo'",
            "CREATE VIEW v2 AS SELECT * FROM t WHERE k2 = 'foo'",
            "CREATE VIEW v3 AS SELECT * FROM t WHERE v = 'bar'||'bas'",
            "CREATE VIEW v4 AS SELECT * FROM t WHERE 'bar'=v and 5+3/2 = k1",
        }, ViewType.UPDATABLE);
        assertViewType(new String[] {
                "CREATE VIEW v1 AS SELECT * FROM t WHERE k1 < 1 AND k2 = 'foo'",
                "CREATE VIEW v2 AS SELECT * FROM t WHERE substr(k2,0,3) = 'foo'",
                "CREATE VIEW v3 AS SELECT * FROM t WHERE v = TO_CHAR(CURRENT_DATE())",
                "CREATE VIEW v4 AS SELECT * FROM t WHERE 'bar'=v or 3 = k1",
            }, ViewType.READ_ONLY);
    }
    
    public void assertViewType(String[] views, ViewType viewType) throws Exception {
        Properties props = new Properties(TestUtil.TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        String ct = "CREATE TABLE t (k1 INTEGER NOT NULL, k2 VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))";
        conn.createStatement().execute(ct);
        
        for (String view : views) {
            conn.createStatement().execute(view);
        }
        
        int count = 0;
        for (PTable table : conn.getPMetaData().getTables().values()) {
            if (table.getType() == PTableType.VIEW) {
                assertEquals(viewType, table.getViewType());
                conn.createStatement().execute("DROP VIEW " + table.getName().getString());
                count++;
            }
        }
        assertEquals(views.length, count);
    }

    @Test
    public void testViewInvalidation() throws Exception {
        Properties props = new Properties(TestUtil.TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        String ct = "CREATE TABLE t (k1 INTEGER NOT NULL, k2 VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))";
        conn.createStatement().execute(ct);
        conn.createStatement().execute("CREATE VIEW v3 AS SELECT * FROM t WHERE v = 'bar'");
        
        // TODO: should it be an error to remove columns from a VIEW that we're defined there?
        // TOOD: should we require an ALTER VIEW instead of ALTER TABLE?
        conn.createStatement().execute("ALTER VIEW v3 DROP COLUMN v");
        try {
            conn.createStatement().executeQuery("SELECT * FROM v3");
            fail();
        } catch (ColumnNotFoundException e) {
            
        }
        
        // No error, as v still exists in t
        conn.createStatement().execute("CREATE VIEW v4 AS SELECT * FROM t WHERE v = 'bas'");

        // No error, even though view is invalid
        conn.createStatement().execute("DROP VIEW v3");
    }


    @Test
    public void testInvalidUpsertSelect() throws Exception {
        Properties props = new Properties(TestUtil.TEST_PROPERTIES);
        PhoenixConnection conn = DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
        conn.createStatement().execute("CREATE TABLE t1 (k1 INTEGER NOT NULL, k2 VARCHAR, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k1,k2))");
        conn.createStatement().execute("CREATE TABLE t2 (k3 INTEGER NOT NULL, v VARCHAR, CONSTRAINT pk PRIMARY KEY (k3))");
        conn.createStatement().execute("CREATE VIEW v1 AS SELECT * FROM t1 WHERE k1 = 1");
        
        try {
            conn.createStatement().executeUpdate("UPSERT INTO v1 SELECT k3,'foo',v FROM t2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }
    }
}
