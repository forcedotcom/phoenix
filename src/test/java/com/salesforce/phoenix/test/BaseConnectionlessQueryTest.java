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
package com.salesforce.phoenix.test;

import static com.salesforce.phoenix.util.TestUtil.*;

import java.sql.DriverManager;
import java.util.Properties;

import org.junit.BeforeClass;

import com.salesforce.phoenix.coprocessor.MetaDataProtocol;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.util.PhoenixRuntime;
import com.salesforce.phoenix.util.TestUtil;



public class BaseConnectionlessQueryTest extends BaseTest {

    public static PTable ATABLE;
    public static PColumn ORGANIZATION_ID;
    public static PColumn ENTITY_ID;
    public static PColumn A_INTEGER;
    public static PColumn A_STRING;
    public static PColumn B_STRING;
    public static PColumn A_DATE;
    public static PColumn A_TIME;
    public static PColumn A_TIMESTAMP;
    public static PColumn X_DECIMAL;
    
    protected static String getUrl() {
        return TestUtil.PHOENIX_CONNECTIONLESS_JDBC_URL;
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        startServer(getUrl());
        ensureTableCreated(getUrl(), ATABLE_NAME);
        ensureTableCreated(getUrl(), FUNKY_NAME);
        ensureTableCreated(getUrl(), PTSDB_NAME);
        ensureTableCreated(getUrl(), MULTI_CF_NAME);
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(MetaDataProtocol.MIN_TABLE_TIMESTAMP));
        PhoenixConnection conn = DriverManager.getConnection(PHOENIX_CONNECTIONLESS_JDBC_URL, props).unwrap(PhoenixConnection.class);
        try {
            PTable table = conn.getPMetaData().getSchema(ATABLE_SCHEMA_NAME).getTable(ATABLE_NAME);
            ATABLE = table;
            ORGANIZATION_ID = table.getColumn("ORGANIZATION_ID");
            ENTITY_ID = table.getColumn("ENTITY_ID");
            A_INTEGER = table.getColumn("A_INTEGER");
            A_STRING = table.getColumn("A_STRING");
            B_STRING = table.getColumn("B_STRING");
            ENTITY_ID = table.getColumn("ENTITY_ID");
            A_DATE = table.getColumn("A_DATE");
            A_TIME = table.getColumn("A_TIME");
            A_TIMESTAMP = table.getColumn("A_TIMESTAMP");
            X_DECIMAL = table.getColumn("X_DECIMAL");
        } finally {
            conn.close();
        }
    }
    

}
