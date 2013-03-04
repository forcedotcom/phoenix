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
package com.salesforce.phoenix.util;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.query.QueryServices;
import com.salesforce.phoenix.query.QueryServicesOptions;



/**
 * Utilities for JDBC
 *
 * @author jtaylor
 * @since 178
 */
public class JDBCUtil {
    
    private JDBCUtil() {
    }

    /**
     * Find the propName by first looking in the url string and if not found,
     * next in the info properties. If not found, null is returned.
     * @param url JDBC connection URL
     * @param info JDBC connection properties
     * @param propName the name of the property to find
     * @return the property value or null if not found
     */
    public static String findProperty(String url, Properties info, String propName) {
        String urlPropName = ";" + propName + "=";
        String propValue = info.getProperty(propName);
        if (propValue == null) {
            int begIndex = url.indexOf(urlPropName);
            if (begIndex >= 0) {
                int endIndex = url.indexOf(';',begIndex + urlPropName.length());
                if (endIndex < 0) {
                    endIndex = url.length();
                }
                propValue = url.substring(begIndex + urlPropName.length(), endIndex);
            }
        }
        return propValue;
    }

    public static Long getCurrentSCN(String url, Properties info) throws SQLException {
        String scnStr = findProperty(url, info, PhoenixRuntime.CURRENT_SCN_ATTRIB);
        return (scnStr == null ? null : Long.parseLong(scnStr));
    }

    /**
     * 
     * Use {@link #getMutateBatchSize(String, Properties, Configuration)} instead
     * @deprecated
     */
    public static int getUpsertBatchSize(String url, Properties info, Configuration config) throws SQLException {
        return getMutateBatchSize(url, info, config);
    }
    
    @SuppressWarnings("deprecation")
    public static int getMutateBatchSize(String url, Properties info, Configuration config) throws SQLException {
        String batchSizeStr = findProperty(url, info, PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB);
        // TODO: remove usage of UPSERT_BATCH_SIZE_ATTRIB in next release
        return (batchSizeStr == null ? config.getInt(QueryServices.MUTATE_BATCH_SIZE_ATTRIB, config.getInt(QueryServices.UPSERT_BATCH_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MUTATE_BATCH_SIZE)) : Integer.parseInt(batchSizeStr));
    }

    public static byte[] getTenantId(String url, Properties info) throws SQLException {
        String tenantId = findProperty(url, info, PhoenixRuntime.TENANT_ID_ATTRIB);
        return (tenantId == null ? null : Bytes.toBytes(tenantId));
    }
}
