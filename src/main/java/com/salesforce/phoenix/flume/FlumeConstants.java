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
package com.salesforce.phoenix.flume;

public final class FlumeConstants {

    /**
     * The Hbase table which the sink should write to.
     */
    public static final String CONFIG_TABLE = "table";
    /**
     * The ddl query for the Hbase table where events are ingested to.
     */
    public static final String CONFIG_TABLE_DDL = "ddl";
    /**
     * Maximum number of events the sink should take from the channel per transaction, if available.
     */
    public static final String CONFIG_BATCHSIZE = "batchSize";
    /**
     * The fully qualified class name of the serializer the sink should use.
     */
    public static final String CONFIG_SERIALIZER = "serializer";
    /**
     * Configuration to pass to the serializer.
     */
    public static final String CONFIG_SERIALIZER_PREFIX = CONFIG_SERIALIZER + ".";

    /**
     * Configuration for the zookeeper quorum.
     */
    public static final String CONFIG_ZK_QUORUM = "zookeeperQuorum";
    
    /**
     * Configuration for the jdbc url.
     */
    public static final String CONFIG_JDBC_URL = "jdbcUrl";

    /**
     * Default batch size .
     */
    public static final Integer DEFAULT_BATCH_SIZE = 100;

    /** Regular expression used to parse groups from event data. */
    public static final String CONFIG_REGULAR_EXPRESSION = "regex";
    public static final String REGEX_DEFAULT = "(.*)";

    /** Whether to ignore case when performing regex matches. */
    public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";
    public static final boolean IGNORE_CASE_DEFAULT = false;

    /** Comma separated list of column names . */
    public static final String CONFIG_COLUMN_NAMES = "columns";

    /** The header columns to persist as columns into the default column family. */
    public static final String CONFIG_HEADER_NAMES = "headers";

    /** The rowkey type generator . */
    public static final String CONFIG_ROWKEY_TYPE_GENERATOR = "rowkeyType";

    /**
     * The default delimiter for columns and headers
     */
    public static final String DEFAULT_COLUMNS_DELIMITER = ",";
}
