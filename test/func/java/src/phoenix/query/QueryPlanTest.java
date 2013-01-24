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
package phoenix.query;

import static org.junit.Assert.assertEquals;
import static phoenix.util.TestUtil.*;

import java.sql.*;
import java.util.Properties;

import org.junit.Test;

public class QueryPlanTest extends BaseConnectedQueryTest {
    private static String getPlan(ResultSet rs) throws SQLException {
        StringBuilder buf = new StringBuilder();
        while (rs.next()) {
            buf.append(rs.getString(1));
            buf.append('\n');
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }
    @Test
    public void testExplainPlan() throws Exception {
        ensureTableCreated(getUrl(), ATABLE_NAME, getDefaultSplits(getOrganizationId()));
        ensureTableCreated(getUrl(), PTSDB_NAME, getDefaultSplits(getOrganizationId()));
        String[] queryPlans = new String[] { 
                "SELECT * FROM atable",
                "CLIENT SERIAL FULL SCAN OVER ATABLE",
                
                "SELECT inst,host FROM PTSDB WHERE regexp_substr(inst, '[^-]+') IN ('na1', 'na2','na3')",
                "CLIENT SERIAL RANGE SCAN OVER PTSDB FROM ('na1') INCLUSIVE TO ('na3') INCLUSIVE\n" + 
                "    SERVER FILTER BY REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na2','na3','na1')",
                
                "SELECT count(*) FROM atable",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER FILTER BY FirstKeyOnlyFilter\n" + 
                "    SERVER AGGREGATE INTO SINGLE ROW",
                
                "SELECT count(*) FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE FROM ('000000000000001','002') EXCLUSIVE TO ('000000000000001','003') INCLUSIVE\n" + 
                "    SERVER FILTER BY FirstKeyOnlyFilter\n" + 
                "    SERVER AGGREGATE INTO SINGLE ROW",
                
                "SELECT a_string FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
                "CLIENT SERIAL RANGE SCAN OVER ATABLE FROM ('000000000000001','002') EXCLUSIVE TO ('000000000000001','003') INCLUSIVE",
                
                "SELECT count(1) FROM atable GROUP BY a_string",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [CF.A_STRING]\n" + 
                "CLIENT MERGE SORT",
                
                "SELECT count(1) FROM atable GROUP BY a_string LIMIT 5",
                "CLIENT SERIAL 5 ROW LIMIT FULL SCAN OVER ATABLE\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [CF.A_STRING]\n" + 
                "CLIENT MERGE SORT",
                
                "SELECT count(1) FROM atable GROUP BY a_string,b_string HAVING max(a_string) = 'a'",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [CF.A_STRING, CF.B_STRING]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT FILTER BY MAX(CF.A_STRING) = 'a'",
                
                "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY ROUND(a_time,'HOUR',2),entity_id HAVING max(a_string) = 'a'",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER FILTER BY CF.A_INTEGER = 1\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(CF.A_TIME)]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT FILTER BY MAX(CF.A_STRING) = 'a'",
                
                "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY a_string,b_string HAVING max(a_string) = 'a' ORDER BY b_string",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER FILTER BY CF.A_INTEGER = 1\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [CF.A_STRING, CF.B_STRING]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT FILTER BY MAX(CF.A_STRING) = 'a'\n" + 
                "CLIENT SORT BY [CF.B_STRING asc nulls first]",
                
                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id = '000000000000002' AND x_integer = 2 AND a_integer < 5 ",
                "CLIENT SERIAL RANGE SCAN OVER ATABLE FROM ('000000000000001','000000000000002') INCLUSIVE TO ('000000000000001','000000000000002') INCLUSIVE\n" + 
                "    SERVER FILTER BY (CF.X_INTEGER = 2 AND CF.A_INTEGER < 5)",
                
                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id != '000000000000002' AND x_integer = 2 AND a_integer < 5 LIMIT 10",
                "CLIENT SERIAL 10 ROW LIMIT RANGE SCAN OVER ATABLE FROM ('000000000000001') INCLUSIVE TO ('000000000000001') INCLUSIVE\n" + 
                "    SERVER FILTER BY (ENTITY_ID != '000000000000002' AND CF.X_INTEGER = 2 AND CF.A_INTEGER < 5)",
                
                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string LIMIT 10",
                "CLIENT SERIAL 10 ROW LIMIT RANGE SCAN OVER ATABLE FROM ('000000000000001') INCLUSIVE TO ('000000000000001') INCLUSIVE\n" + 
                "CLIENT SORT BY [CF.A_STRING asc nulls first]",
                
                "SELECT max(a_integer) FROM atable WHERE organization_id = '000000000000001' GROUP BY organization_id,entity_id,ROUND(a_date,'HOUR') ORDER BY entity_id LIMIT 10",
                "CLIENT SERIAL 10 ROW LIMIT RANGE SCAN OVER ATABLE FROM ('000000000000001') INCLUSIVE TO ('000000000000001') INCLUSIVE\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ORGANIZATION_ID, ENTITY_ID, ROUND(CF.A_DATE)]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT SORT BY [ENTITY_ID asc nulls first]",
                
                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string LIMIT 10",
                "CLIENT SERIAL 10 ROW LIMIT RANGE SCAN OVER ATABLE FROM ('000000000000001') INCLUSIVE TO ('000000000000001') INCLUSIVE\n" + 
                "CLIENT SORT BY [CF.A_STRING asc nulls first]",
        };
        for (int i = 0; i < queryPlans.length; i+=2) {
            String query = queryPlans[i];
            String plan = queryPlans[i+1];
            Properties props = new Properties();
            Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            try {
                Statement statement = conn.createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + query);
                // TODO: figure out a way of verifying that query isn't run during explain execution
                assertEquals(query, plan, getPlan(rs));
            } catch (Exception e) {
                throw new Exception(query + e.getMessage(), e);
            } finally {
                conn.close();
            }
        }
    }

}
