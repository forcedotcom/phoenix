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

import static org.junit.Assert.*;
import static phoenix.util.TestUtil.*;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import phoenix.query.QueryConstants;
import phoenix.util.PhoenixRuntime;

public class ExecuteStatementsTest extends BaseHBaseManagedTimeTest {
    
    // TODO: move this last once we delete data on a drop table command
    @Test
    public void testExecuteStatements() throws Exception {
        String tenantId = getOrganizationId();
        initATableValues(tenantId, getDefaultSplits(tenantId));
        String statements = 
            "create table if not exists " + ATABLE_NAME + // Shouldn't error out b/c of if not exists clause
            "   (organization_id char(15) not null, \n" + 
            "    entity_id char(15) not null)\n" + 
            "    cf(a_string varchar(100),\n" + 
            "       b_string varchar(100));\n" + 
            "create table " + PTSDB_NAME +
            "   (inst varchar null,\n" + 
            "    host varchar null,\n" + 
            "    date date not null)\n" + 
            "    a(val decimal)\n" +
            "    split on (?,?,?);\n" +
            "alter table " + PTSDB_NAME + " add if not exists a(val decimal);\n" +  // Shouldn't error out b/c of if not exists clause
            "alter table " + PTSDB_NAME + " drop column if exists blah;\n" +  // Shouldn't error out b/c of if exists clause
            "drop table if exists FOO.BAR;\n" + // Shouldn't error out b/c of if exists clause
            "UPSERT INTO " + PTSDB_NAME + "(date, val, host) " +
            "    SELECT current_date(), x_integer+2, entity_id FROM ATABLE WHERE a_integer >= ?;" +
            "UPSERT INTO " + PTSDB_NAME + "(date, val, inst)\n" +
            "    SELECT date+1, val*10, host FROM " + PTSDB_NAME + ";";
        
        Date now = new Date(System.currentTimeMillis());
        Connection conn = DriverManager.getConnection(getUrl());
        conn.setAutoCommit(true);
        List<Object> binds = Arrays.<Object>asList("a","j","s", 6);
        int nStatements = PhoenixRuntime.executeStatements(conn, new StringReader(statements), binds);
        assertEquals(7, nStatements);

        Date then = new Date(System.currentTimeMillis() + QueryConstants.MILLIS_IN_DAY);
        String query = "SELECT host,inst, date,val FROM " + PTSDB_NAME + " where inst is not null";
        PreparedStatement statement = conn.prepareStatement(query);
        
        ResultSet rs = statement.executeQuery();
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW6, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertEquals(null, rs.getBigDecimal(4));
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW7, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(70).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW8, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(60).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertTrue (rs.next());
        assertEquals(null, rs.getString(1));
        assertEquals(ROW9, rs.getString(2));
        assertTrue(rs.getDate(3).after(now) && rs.getDate(3).before(then));
        assertTrue(BigDecimal.valueOf(50).compareTo(rs.getBigDecimal(4)) == 0);
        
        assertFalse(rs.next());
        conn.close();
    }
}
