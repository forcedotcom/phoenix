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
package com.salesforce.phoenix.flume.serializer;

import static com.salesforce.phoenix.flume.FlumeConstants.CONFIG_COLUMN_NAMES;
import static com.salesforce.phoenix.flume.FlumeConstants.CONFIG_HEADER_NAMES;
import static com.salesforce.phoenix.flume.FlumeConstants.CONFIG_ROWKEY_TYPE_GENERATOR;
import static com.salesforce.phoenix.flume.FlumeConstants.DEFAULT_COLUMNS_DELIMITER;
import static com.salesforce.phoenix.util.PhoenixRuntime.UPSERT_BATCH_SIZE_ATTRIB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.flume.DefaultKeyGenerator;
import com.salesforce.phoenix.flume.FlumeConstants;
import com.salesforce.phoenix.flume.KeyGenerator;
import com.salesforce.phoenix.flume.SchemaHandler;
import com.salesforce.phoenix.util.ColumnInfo;
import com.salesforce.phoenix.util.QueryUtil;
import com.salesforce.phoenix.util.SchemaUtil;

public abstract class BaseEventSerializer implements EventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(BaseEventSerializer.class);
    
    protected Connection connection;
    protected String fullTableName;
    protected ColumnInfo[] columnMetadata;
    protected boolean autoGenerateKey = false;
    protected KeyGenerator  keyGenerator;
    protected List<String>  colNames = Lists.newArrayListWithExpectedSize(10);
    protected List<String>  headers  = Lists.newArrayListWithExpectedSize(5);
    protected String upsertStatement;
    private   String jdbcUrl;
    private   Integer batchSize;
    private   String  createTableDdl;


    
    
    
    @Override
    public void configure(Context context) {
        
        this.createTableDdl = context.getString(FlumeConstants.CONFIG_TABLE_DDL);
        this.fullTableName = context.getString(FlumeConstants.CONFIG_TABLE);
        final String zookeeperQuorum = context.getString(FlumeConstants.CONFIG_ZK_QUORUM);
        final String ipJdbcURL = context.getString(FlumeConstants.CONFIG_JDBC_URL);
        this.batchSize = context.getInteger(FlumeConstants.CONFIG_BATCHSIZE, FlumeConstants.DEFAULT_BATCH_SIZE);
        final String columnNames = context.getString(CONFIG_COLUMN_NAMES);
        final String headersStr = context.getString(CONFIG_HEADER_NAMES);
        final String keyGeneratorType = context.getString(CONFIG_ROWKEY_TYPE_GENERATOR);
       
        Preconditions.checkNotNull(this.fullTableName,"Table name cannot be empty, please specify in the configuration file");
        if(!Strings.isNullOrEmpty(zookeeperQuorum)) {
            this.jdbcUrl = QueryUtil.getUrl(zookeeperQuorum);
        }
        if(!Strings.isNullOrEmpty(ipJdbcURL)) {
            this.jdbcUrl = ipJdbcURL;
        }
        Preconditions.checkNotNull(this.jdbcUrl,"Please specify either the zookeeper quorum or the jdbc url in the configuration file");
        Preconditions.checkNotNull(columnNames,"Column names cannot be empty, please specify in configuration file");
        for(String s : Splitter.on(DEFAULT_COLUMNS_DELIMITER).split(columnNames)) {
           colNames.add(s);
        }
        
         if(!Strings.isNullOrEmpty(headersStr)) {
            for(String s : Splitter.on(DEFAULT_COLUMNS_DELIMITER).split(headersStr)) {
                headers.add(s);
             }
        }
      
        if(!Strings.isNullOrEmpty(keyGeneratorType)) {
            try {
                keyGenerator =  DefaultKeyGenerator.valueOf(keyGeneratorType.toUpperCase());
                this.autoGenerateKey = true;
            } catch(IllegalArgumentException iae) {
                logger.error("An invalid key generator {} was specified in configuration file. Specify one of {}",keyGeneratorType,DefaultKeyGenerator.values());
                Throwables.propagate(iae);
            } 
        }
        
        logger.debug(" the jdbcUrl configured is {}",jdbcUrl);
        logger.debug(" columns configured are {}",colNames.toString());
        logger.debug(" headers configured are {}",headersStr);
        logger.debug(" the keyGenerator configured is {} ",keyGeneratorType);

        doConfigure(context);
        
    }
    
    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP
        
    }
    
    
    @Override
    public void initialize() throws SQLException {
        final Properties props = new Properties();
        props.setProperty(UPSERT_BATCH_SIZE_ATTRIB, String.valueOf(this.batchSize)); 
        ResultSet rs = null;
        try {
            this.connection = DriverManager.getConnection(this.jdbcUrl, props);
            this.connection.setAutoCommit(false);
            if(this.createTableDdl != null) {
                 SchemaHandler.createTable(connection,createTableDdl);
            }
      
            
            final Map<String,Integer> allColumnsInfoMap = Maps.newLinkedHashMap();
            final String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
            final String tableName  = SchemaUtil.getTableNameFromFullName(fullTableName);
            
            String rowkey = null;
            String  cq = null;
            String  cf = null;
            Integer dt = null;
            rs = connection.getMetaData().getColumns(null, schemaName, tableName, null);
            while (rs.next()) {
                cf = rs.getString(QueryUtil.COLUMN_FAMILY_POSITION);
                cq = rs.getString(QueryUtil.COLUMN_NAME_POSITION);
                dt = rs.getInt(QueryUtil.DATA_TYPE_POSITION);
                if(Strings.isNullOrEmpty(cf)) {
                    rowkey = cq; // this is required only when row key is auto generated
                }
                allColumnsInfoMap.put(SchemaUtil.getColumnDisplayName(cf, cq), dt);
             }
            
            //can happen when table not found in Hbase.
            if(allColumnsInfoMap.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TABLE_UNDEFINED)
                        .setTableName(tableName).build().buildException();
            }
       
            int colSize = colNames.size();
            int headersSize = headers.size();
            int totalSize = colSize + headersSize + ( autoGenerateKey ? 1 : 0);
            columnMetadata = new ColumnInfo[totalSize] ;
           
            int position = 0;
            position = this.addToColumnMetadataInfo(colNames, allColumnsInfoMap, position);
            position = this.addToColumnMetadataInfo(headers,  allColumnsInfoMap, position);
           
            if(autoGenerateKey) {
                Integer sqlType = allColumnsInfoMap.get(rowkey);
                if (sqlType == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                         .setColumnName(rowkey).setTableName(fullTableName).build().buildException();
                }
                columnMetadata[position] = new ColumnInfo(rowkey, sqlType);
                position++;
            }
            
            this.upsertStatement = QueryUtil.constructUpsertStatement(columnMetadata, fullTableName, columnMetadata.length);
            logger.info(" the upsert statement is {} " ,this.upsertStatement);
            
        }  catch (SQLException e) {
            logger.error("error {} occurred during initializing connection ",e.getMessage());
            throw e;
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
        doInitialize();
    }
    
    private int addToColumnMetadataInfo(final List<String> columns , final Map<String,Integer> allColumnsInfoMap, int position) throws SQLException {
        Preconditions.checkNotNull(columns);
        Preconditions.checkNotNull(allColumnsInfoMap);
       for (int i = 0 ; i < columns.size() ; i++) {
            String columnName = SchemaUtil.normalizeIdentifier(columns.get(i).trim());
            Integer sqlType = allColumnsInfoMap.get(columnName);
            if (sqlType == null) {
                   throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                        .setColumnName(columnName).setTableName(this.fullTableName).build().buildException();
            } else {
                columnMetadata[position] = new ColumnInfo(columnName, sqlType);
                position++;
            }
       }
       return position;
    }
    
    public abstract void doConfigure(Context context);
    
    public abstract void doInitialize() throws SQLException;
    
    
    @Override
    public void close() {
        if(connection != null) {
            try {
               connection.close();
         } catch (SQLException e) {
            logger.error(" Error while closing connection {} ");
         }
       }
    }
}
