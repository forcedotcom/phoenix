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
package com.salesforce.phoenix.parse;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import org.junit.Test;

import com.salesforce.phoenix.parse.SQLParser;


public class QueryParserTest {
    @Test
    public void testParsePreQuery0() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select a from b\n" +
            "where ((ind.name = 'X')" +
            "and rownum <= (1000 + 1000))\n"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParsePreQuery1() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ count(1) from core.search_name_lookup ind\n" +
            "where( (ind.name = 'X'\n" +
            "and rownum <= 1 + 2)\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't'))"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParsePreQuery2() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ count(1) from core.custom_index_value ind\n" + 
            "where (ind.string_value in ('a', 'b', 'c', 'd'))\n" + 
            "and rownum <= ( 3 + 1 )\n" + 
            "and (ind.organization_id = '000000000000000')\n" + 
            "and (ind.key_prefix = '00T')\n" + 
            "and (ind.deleted = '0')\n" + 
            "and (ind.index_num = 1)"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParsePreQuery3() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ count(1) from core.custom_index_value ind\n" + 
            "where (ind.number_value > 3)\n" + 
            "and rownum <= 1000\n" + 
            "and (ind.organization_id = '000000000000000')\n" + 
            "and (ind.key_prefix = '001'\n" + 
            "and (ind.deleted = '0'))\n" + 
            "and (ind.index_num = 2)"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParsePreQuery4() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*+ index(t iecustom_entity_data_created) */ /*gatherSlowStats*/ count(1) from core.custom_entity_data t\n" + 
            "where (t.created_date > to_date('01/01/2001'))\n" + 
            "and rownum <= 4500\n" + 
            "and (t.organization_id = '000000000000000')\n" + 
            "and (t.key_prefix = '001')"
            ));
        parser.parseStatement();
    }

    @Test
    public void testCountDistinctQuery() throws Exception {
        try {
            SQLParser parser = new SQLParser(new StringReader(
                "select count(distinct foo) from core.custom_entity_data t\n" + 
                "where (t.created_date > to_date('01/01/2001'))\n" + 
                "and (t.organization_id = '000000000000000')\n" + 
                "and (t.key_prefix = '001')\n" +
                "limit 4500"
                ));
            parser.parseStatement();
            fail();
        } catch (SQLFeatureNotSupportedException e) {
        }
    }

    @Test
    public void testIsNullQuery() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select count(foo) from core.custom_entity_data t\n" + 
            "where (t.created_date is null)\n" + 
            "and (t.organization_id is not null)\n"
            ));
        parser.parseStatement();
    }

    @Test
    public void testAsInColumnAlias() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select count(foo) AS c from core.custom_entity_data t\n" + 
            "where (t.created_date is null)\n" + 
            "and (t.organization_id is not null)\n"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParseJoin1() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*SOQL*/ \"Id\"\n" + 
            "from (select /*+ ordered index(cft) */\n" + 
            "cft.val188 \"Marketing_Offer_Code__c\",\n" + 
            "t.account_id \"Id\"\n" + 
            "from sales.account_cfdata cft,\n" + 
            "sales.account t\n" + 
            "where (cft.account_cfdata_id = t.account_id)\n" + 
            "and (cft.organization_id = '00D300000000XHP')\n" + 
            "and (t.organization_id = '00D300000000XHP')\n" + 
            "and (t.deleted = '0')\n" + 
            "and (t.account_id != '000000000000000'))\n" + 
            "where (\"Marketing_Offer_Code__c\" = 'FSCR')"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParseJoin2() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*rptacctlist 00O40000002C3of*/ \"00N40000001M8VK\",\n" + 
            "\"00N40000001M8VK.ID\",\n" + 
            "\"00N30000000r0K2\",\n" + 
            "\"00N30000000jgjo\"\n" + 
            "from (select /*+ ordered use_hash(aval368) index(cfa) */\n" + 
            "a.record_type_id \"RECORDTYPE\",\n" + 
            "aval368.last_name,aval368.first_name || ' ' || aval368.last_name,aval368.name \"00N40000001M8VK\",\n" + 
            "a.last_update \"LAST_UPDATE\",\n" + 
            "cfa.val368 \"00N40000001M8VK.ID\",\n" + 
            "TO_DATE(cfa.val282) \"00N30000000r0K2\",\n" + 
            "cfa.val252 \"00N30000000jgjo\"\n" + 
            "from sales.account a,\n" + 
            "sales.account_cfdata cfa,\n" + 
            "core.name_denorm aval368\n" + 
            "where (cfa.account_cfdata_id = a.account_id)\n" + 
            "and (aval368.entity_id (+) = cfa.val368)\n" + 
            "and (a.deleted = '0')\n" + 
            "and (a.organization_id = '00D300000000EaE')\n" + 
            "and (a.account_id <> '000000000000000')\n" + 
            "and (cfa.organization_id = '00D300000000EaE')\n" + 
            "and (aval368.organization_id (+) = '00D300000000EaE')\n" + 
            "and (aval368.entity_id (+) like '005%'))\n" + 
            "where (\"RECORDTYPE\" = '0123000000002Gv')\n" + 
            "AND (\"00N40000001M8VK\" is null or \"00N40000001M8VK\" in ('BRIAN IRWIN', 'BRIAN MILLER', 'COLLEEN HORNYAK', 'ERNIE ZAVORAL JR', 'JAMIE TRIMBUR', 'JOE ANTESBERGER', 'MICHAEL HYTLA', 'NATHAN DELSIGNORE', 'SANJAY GANDHI', 'TOM BASHIOUM'))\n" + 
            "AND (\"LAST_UPDATE\" >= to_date('2009-08-01 07:00:00'))"
            ));
        parser.parseStatement();
    }
    
    @Test
    public void testNegative1() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ count(1) core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parser.parseStatement();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 603 (42P00): Syntax error. Mismatched input. Expecting \"FROM\", got \".\" at line 1, column 41."));
        }
    }

    @Test
    public void testNegative2() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "seelect /*gatherSlowStats*/ count(1) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parser.parseStatement();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"seelect\" at line 1, column 1."));
        }
    }

    @Test
    public void testNegative3() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ count(1) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't'))"
            ));
        try {
            parser.parseStatement();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 603 (42P00): Syntax error. Unexpected input. Expecting \"EOF\", got \")\" at line 6, column 26."));
        }
    }

    @Test
    public void testNegativeCountDistinct() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ max( distinct 1) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parser.parseStatement();
            fail();
        } catch (SQLFeatureNotSupportedException e) {
            // expected
        }
    }

    @Test
    public void testNegativeCountStar() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ max(*) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parser.parseStatement();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"*\" at line 1, column 32."));
        }
    }

    @Test
    public void testUnknownFunction() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ bogus_function(ind.key_prefix) from core.search_name_lookup ind\n" +
            "where (ind.name = 'X')\n" +
            "and rownum <= 2000\n" +
            "and (ind.organization_id = '000000000000000')\n" +
            "and (ind.key_prefix = '00T')\n" +
            "and (ind.name_type = 't')"
            ));
        try {
            parser.parseStatement();
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 604 (42P00): Syntax error. Unknown function: \"BOGUS_FUNCTION\"."));
        }
    }

    @Test
    public void testNegativeNonBooleanWhere() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select /*gatherSlowStats*/ max( distinct 1) from core.search_name_lookup ind\n" +
            "where 1"
            ));
        try {
            parser.parseStatement();
            fail();
        } catch (SQLFeatureNotSupportedException e) {
            // expected
        }
    }
    
    @Test
    public void testCommentQuery() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select a from b -- here we come\n" +
            "where ((ind.name = 'X') // to save the day\n" +
            "and rownum /* won't run */ <= (1000 + 1000))\n"
            ));
        parser.parseStatement();
    }

    @Test
    public void testQuoteEscapeQuery() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select a from b\n" +
            "where ind.name = 'X''Y'\n"
            ));
        parser.parseStatement();
    }

    @Test
    public void testSubtractionInSelect() throws Exception {
        SQLParser parser = new SQLParser(new StringReader(
            "select a, 3-1-2, -4- -1-1 from b\n" +
            "where d = c - 1\n"
            ));
        parser.parseStatement();
    }

    @Test
    public void testParsingStatementWithMispellToken() throws Exception {
        try {
            SQLParser parser = new SQLParser(new StringReader(
                    "selects a from b\n" +
                    "where e = d\n"));
            parser.parseStatement();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"selects\" at line 1, column 1."));
        }
        try {
            SQLParser parser = new SQLParser(new StringReader(
                    "select a froms b\n" +
                    "where e = d\n"));
            parser.parseStatement();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 602 (42P00): Syntax error. Missing \"FROM\" at line 1, column 16."));
        }
    }

    @Test
    public void testParsingStatementWithExtraToken() throws Exception {
        try {
            SQLParser parser = new SQLParser(new StringReader(
                    "select a,, from b\n" +
                    "where e = d\n"));
            parser.parseStatement();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \",\" at line 1, column 10."));
        }
        try {
            SQLParser parser = new SQLParser(new StringReader(
                    "select a from from b\n" +
                    "where e = d\n"));
            parser.parseStatement();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"from\" at line 1, column 15."));
        }
    }

    @Test
    public void testParsingStatementWithMissingToken() throws Exception {
        try {
            SQLParser parser = new SQLParser(new StringReader(
                    "select a b\n" +
                    "where e = d\n"));
            parser.parseStatement();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 603 (42P00): Syntax error. Mismatched input. Expecting \"FROM\", got \"where\" at line 2, column 1."));
        }
        try {
            SQLParser parser = new SQLParser(new StringReader(
                    "select a from b\n" +
                    "where d\n"));
            parser.parseStatement();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("ERROR 601 (42P00): Syntax error. Encountered \"d\" at line 2, column 7."));
        }
    }
}
