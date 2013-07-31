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
package com.salesforce.phoenix.end2end;

import static com.salesforce.phoenix.util.TestUtil.*;
import static org.junit.Assert.assertEquals;

import java.sql.*;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.util.QueryUtil;
import com.salesforce.phoenix.util.ReadOnlyProps;

public class QueryPlanTest extends BaseConnectedQueryTest {
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        // Override date format so we don't have a bunch of zeros
        props.put(QueryServices.DATE_FORMAT_ATTRIB, "yyyy-MM-dd");
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testExplainPlan() throws Exception {
        ensureTableCreated(getUrl(), ATABLE_NAME, getDefaultSplits(getOrganizationId()));
        ensureTableCreated(getUrl(), PTSDB_NAME, getDefaultSplits(getOrganizationId()));
        ensureTableCreated(getUrl(), PTSDB3_NAME, getDefaultSplits(getOrganizationId()));
        String[] queryPlans = new String[] {
                "SELECT * FROM atable",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE",

                "SELECT inst,host FROM PTSDB WHERE REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1', 'na2','na3')", // REVIEW: should this use skip scan given the regexpr_substr
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 3 RANGES OVER PTSDB ['na1'-'na2')...['na3'-'na4')\n" + 
                "    SERVER FILTER BY REGEXP_SUBSTR(INST, '[^-]+', 1) IN ('na1','na2','na3')",

                "SELECT inst,host FROM PTSDB WHERE inst IN ('na1', 'na2','na3') AND host IN ('a','b') AND date >= to_date('2013-01-01') AND date < to_date('2013-01-02')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 6 RANGES OVER PTSDB 'na1'...'na3','a'...'b',['2013-01-01'-'2013-01-02')",

                "SELECT inst,host FROM PTSDB WHERE inst LIKE 'na%' AND host IN ('a','b') AND date >= to_date('2013-01-01') AND date < to_date('2013-01-02')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 RANGES OVER PTSDB ['na'-'nb'),'a'...'b',['2013-01-01'-'2013-01-02')",

                "SELECT host FROM PTSDB3 WHERE host IN ('na1', 'na2','na3')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 3 KEYS OVER PTSDB3 'na3'...'na1'",

                "SELECT count(*) FROM atable",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER FILTER BY FirstKeyOnlyFilter\n" +
                "    SERVER AGGREGATE INTO SINGLE ROW",

                // TODO: review: why does this change with parallelized non aggregate queries?
                "SELECT count(*) FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE '000000000000001',['003'-'004')\n" + 
                "    SERVER FILTER BY FirstKeyOnlyFilter\n" + 
                "    SERVER AGGREGATE INTO SINGLE ROW",

                "SELECT a_string FROM atable WHERE organization_id='000000000000001' AND SUBSTR(entity_id,1,3) > '002' AND SUBSTR(entity_id,1,3) <= '003'",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE '000000000000001',['003'-'004')",

                "SELECT count(1) FROM atable GROUP BY a_string",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]\n" +
                "CLIENT MERGE SORT",

                "SELECT count(1) FROM atable GROUP BY a_string LIMIT 5",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT 5 ROW LIMIT",

                "SELECT a_string FROM atable ORDER BY a_string DESC LIMIT 3",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" + 
                "    SERVER TOP 3 ROWS SORTED BY [A_STRING DESC]\n" + 
                "CLIENT MERGE SORT",

                "SELECT count(1) FROM atable GROUP BY a_string,b_string HAVING max(a_string) = 'a'",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT FILTER BY MAX(A_STRING) = 'a'",

                "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY ROUND(a_time,'HOUR',2),entity_id HAVING max(a_string) = 'a'",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER FILTER BY A_INTEGER = 1\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ENTITY_ID, ROUND(A_TIME)]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT FILTER BY MAX(A_STRING) = 'a'",

                "SELECT count(1) FROM atable WHERE a_integer = 1 GROUP BY a_string,b_string HAVING max(a_string) = 'a' ORDER BY b_string",
                "CLIENT PARALLEL 4-WAY FULL SCAN OVER ATABLE\n" +
                "    SERVER FILTER BY A_INTEGER = 1\n" +
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [A_STRING, B_STRING]\n" +
                "CLIENT MERGE SORT\n" +
                "CLIENT FILTER BY MAX(A_STRING) = 'a'\n" +
                "CLIENT SORTED BY [B_STRING]",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id = '000000000000002' AND x_integer = 2 AND a_integer < 5 ",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE '000000000000001','000000000000002'\n" + 
                "    SERVER FILTER BY (X_INTEGER = 2 AND A_INTEGER < 5)",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' AND entity_id != '000000000000002' AND x_integer = 2 AND a_integer < 5 LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE '000000000000001'\n" + 
                "    SERVER FILTER BY (ENTITY_ID != '000000000000002' AND X_INTEGER = 2 AND A_INTEGER < 5)\n" + 
                "    SERVER 10 ROW LIMIT\n" + 
                "CLIENT 10 ROW LIMIT",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string ASC NULLS FIRST LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE '000000000000001'\n" + 
                "    SERVER TOP 10 ROWS SORTED BY [A_STRING]\n" + 
                "CLIENT MERGE SORT",

                "SELECT max(a_integer) FROM atable WHERE organization_id = '000000000000001' GROUP BY organization_id,entity_id,ROUND(a_date,'HOUR') ORDER BY entity_id NULLS LAST LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE '000000000000001'\n" + 
                "    SERVER AGGREGATE INTO DISTINCT ROWS BY [ORGANIZATION_ID, ENTITY_ID, ROUND(A_DATE)]\n" + 
                "CLIENT MERGE SORT\n" + 
                "CLIENT TOP 10 ROWS SORTED BY [ENTITY_ID NULLS LAST]",

                "SELECT a_string,b_string FROM atable WHERE organization_id = '000000000000001' ORDER BY a_string DESC NULLS LAST LIMIT 10",
                "CLIENT PARALLEL 1-WAY RANGE SCAN OVER ATABLE '000000000000001'\n" + 
                "    SERVER TOP 10 ROWS SORTED BY [A_STRING DESC NULLS LAST]\n" + 
                "CLIENT MERGE SORT",

                "SELECT a_string,b_string FROM atable WHERE organization_id IN ('000000000000001', '000000000000005')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 2 KEYS OVER ATABLE '000000000000001'...'000000000000005'",

                "SELECT a_string,b_string FROM atable WHERE organization_id IN ('00D000000000001', '00D000000000005') AND entity_id IN('00E00000000000X','00E00000000000Z')",
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 4 KEYS OVER ATABLE '00D000000000001'...'00D000000000005','00E00000000000X'...'00E00000000000Z'",
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
                assertEquals(query, plan, QueryUtil.getExplainPlan(rs));
            } catch (Exception e) {
                throw new Exception(query + ": "+ e.getMessage(), e);
            } finally {
                conn.close();
            }
        }
    }

}
