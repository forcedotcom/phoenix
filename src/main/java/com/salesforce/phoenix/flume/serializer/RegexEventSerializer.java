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

import static com.salesforce.phoenix.flume.FlumeConstants.COL_NAME_CONFIG;
import static com.salesforce.phoenix.flume.FlumeConstants.DEFAULT_COLUMNS_DELIMITER;
import static com.salesforce.phoenix.flume.FlumeConstants.HEADER_NAME_CONFIG;
import static com.salesforce.phoenix.flume.FlumeConstants.IGNORE_CASE_CONFIG;
import static com.salesforce.phoenix.flume.FlumeConstants.IGNORE_CASE_DEFAULT;
import static com.salesforce.phoenix.flume.FlumeConstants.REGEX_CONFIG;
import static com.salesforce.phoenix.flume.FlumeConstants.REGEX_DEFAULT;
import static com.salesforce.phoenix.flume.FlumeConstants.ROWKEY_TYPE_CONFIG;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
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
import com.salesforce.phoenix.flume.KeyGenerator;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.TableNotFoundException;
import com.salesforce.phoenix.util.ColumnInfo;
import com.salesforce.phoenix.util.QueryUtil;
import com.salesforce.phoenix.util.SchemaUtil;

public class RegexEventSerializer implements EventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(RegexEventSerializer.class);
  
    private String upsertStatement;
    private List<String> colNames = Lists.newArrayListWithExpectedSize(10);
    private List<String> headers  = Lists.newArrayListWithExpectedSize(5);
    private ColumnInfo[] columnMetadata ;
    private Pattern inputPattern;
    private boolean autoGenerateKey = false;
    private KeyGenerator  keyGenerator;
    private String table;
    
    
    /**
     * 
     */
    @Override
    public void configure(Context context) {
        final String regex    = context.getString(REGEX_CONFIG, REGEX_DEFAULT);
        final boolean regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,IGNORE_CASE_DEFAULT);
        inputPattern = Pattern.compile(regex, Pattern.DOTALL + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
        
        final String columnNames = context.getString(COL_NAME_CONFIG);
        Preconditions.checkNotNull(columnNames,"Column names cannot be empty, please specify in configuration file");
        for(String s : Splitter.on(DEFAULT_COLUMNS_DELIMITER).split(columnNames)) {
           colNames.add(s);
        }
        
        logger.error(" columns configured are {}",colNames.toString());
        
        final String headersStr = context.getString(HEADER_NAME_CONFIG);
        if(!Strings.isNullOrEmpty(headersStr)) {
            for(String s : Splitter.on(DEFAULT_COLUMNS_DELIMITER).split(headersStr)) {
                headers.add(s);
             }
        }
        logger.error(" headers configured are {}",headersStr);
        final String keyGeneratorType = context.getString(ROWKEY_TYPE_CONFIG);
        logger.error(" the keyGenerator is {} passed as argment ",keyGeneratorType);
        if(!Strings.isNullOrEmpty(keyGeneratorType)) {
            try {
                keyGenerator =  DefaultKeyGenerator.valueOf(keyGeneratorType.toUpperCase());
                this.autoGenerateKey = true;
            } catch(IllegalArgumentException iae) {
                logger.error("An invalid key generator {} was specified in configuration file. Specify one of {}",keyGeneratorType,DefaultKeyGenerator.values());
                Throwables.propagate(iae);
            } 
        }
    }

     @Override
    public void configure(ComponentConfiguration conf) {
         // NO_OP
    }
    
    /**
     * 
     */
    @Override
    public void initialize(final Connection connection , final String tableName) throws SQLException {
    
        ResultSet rs = null;
        try {
           
            //a) generate the column name to data type mapping of all the columns for the table
            //b) for the list of columns passed through configuration , generate the ColumnInfo
            //c) for the ColumnInfo information , generate a upsert statement.
            
            //a)
            final Map<String,Integer> allColumnsInfoMap = Maps.newLinkedHashMap();
            final String schemaName = SchemaUtil.getSchemaNameFromFullName(tableName);
            table = SchemaUtil.getTableNameFromFullName(tableName);
            logger.error(" the table in initialize is {}",table);
            String rowkey = null;
            String  cq = null;
            String  cf = null;
            Integer dt = null;
            rs = connection.getMetaData().getColumns(null, schemaName, table, null);
            while (rs.next()) {
                cf = rs.getString(QueryUtil.COLUMN_FAMILY_POSITION);
                cq = rs.getString(QueryUtil.COLUMN_NAME_POSITION);
                dt = rs.getInt(QueryUtil.DATA_TYPE_POSITION);
                if(Strings.isNullOrEmpty(cf)) {
                    rowkey = cq; // this is required only when row key is auto generated
                }
                allColumnsInfoMap.put(SchemaUtil.getColumnDisplayName(cf, cq), dt);
             }
                 
            //b)
            int colSize = colNames.size();
            int headersSize = headers.size();
            int totalSize = colSize + headersSize + ( autoGenerateKey ? 1 : 0);
            columnMetadata = new ColumnInfo[totalSize] ;
            Integer position = 0;
            
            this.addToColumnMetadataInfo(colNames, allColumnsInfoMap, position);
            this.addToColumnMetadataInfo(headers, allColumnsInfoMap, position);
            logger.error(" the table in initialize autoGenerateKey {}",autoGenerateKey);
            if(autoGenerateKey) {
                Integer sqlType = allColumnsInfoMap.get(rowkey);
                if (sqlType == null) {
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.PRIMARY_KEY_MISSING)
                         .setColumnName(rowkey).setTableName(tableName).build().buildException();
                }
                columnMetadata[position] = new ColumnInfo(rowkey, sqlType);
                position++;
            }
            
            logger.error(" the column metadata length is {}",columnMetadata.length);
            
            //c) 
            this.upsertStatement = QueryUtil.constructUpsertStatement(columnMetadata, tableName, columnMetadata.length);
            logger.error(" the upsert statement is {} " ,this.upsertStatement);
            
        } catch(TableNotFoundException ex){
            logger.error(" the table {} doesn't exist in Hbase.",tableName);
            throw ex;
        } catch (SQLException e) {
            logger.error("error {} occurred during initializing connection ",e.getMessage());
            throw e;
        } finally {
            if(rs != null) {
                rs.close();
            }
        }
       
    }
    
    private void addToColumnMetadataInfo(final List<String> columns , final Map<String,Integer> allColumnsInfoMap, Integer position) throws SQLException {
        Preconditions.checkNotNull(columns);
        Preconditions.checkNotNull(allColumnsInfoMap);
        for (int i = 0 ; i < columns.size() ; i++) {
            String columnName = SchemaUtil.normalizeIdentifier(colNames.get(i).trim());
            Integer sqlType = allColumnsInfoMap.get(columnName);
            if (sqlType == null) {
                   throw new SQLExceptionInfo.Builder(SQLExceptionCode.COLUMN_NOT_FOUND)
                        .setColumnName(columnName).setTableName(table).build().buildException();
            } else {
                columnMetadata[position] = new ColumnInfo(columnName, sqlType);
                position++;
            }
        }
        
    }

   
    @Override
    public void upsertEvents(List<Event> events, Connection connection) throws SQLException {
       Preconditions.checkNotNull(events);
       Preconditions.checkNotNull(connection);
       Preconditions.checkNotNull(this.upsertStatement);
       
       PreparedStatement colUpsert = connection.prepareStatement(upsertStatement);
       boolean wasAutoCommit = connection.getAutoCommit();
       connection.setAutoCommit(false);
       
       String value = null;
       Integer sqlType = null;
       try {
           for(Event event : events) {
               byte [] payloadBytes = event.getBody();
               String payload = new String(payloadBytes);
               Matcher m = inputPattern.matcher(payload);
               
               if (!m.matches()) {
                 logger.error("payload {} doesn't match the pattern {} ", payload, inputPattern.toString());  
                 continue;
               }
               if (m.groupCount() != colNames.size()) {
                 logger.error("payload {} size doesn't match the pattern {} ", m.groupCount(), colNames.size());
                 continue;
               }
               int index = 1 ;
               int offset = 0;
               for (int i = 0 ; i <  colNames.size() ; i++,offset++) {
                   if (columnMetadata[offset] == null ) {
                       continue;
                   }
                   
                   value = m.group(i + 1);
                   sqlType = columnMetadata[offset].getSqlType();
                   Object upsertValue = PDataType.fromSqlType(sqlType).toObject(value);
                   if (upsertValue != null) {
                       colUpsert.setObject(index++, upsertValue, sqlType);
                   } else {
                       colUpsert.setNull(index++, sqlType);
                   }
                }
               
               //add headers if necessary
               Map<String,String> headerValues = event.getHeaders();
               for(int i = 0 ; i < headers.size() ; i++ , offset++) {
                
                   String headerName  = headers.get(i);
                   String headerValue = headerValues.get(headerName);
                   sqlType = columnMetadata[offset].getSqlType();
                   Object upsertValue = PDataType.fromSqlType(sqlType).toObject(headerValue);
                   if (upsertValue != null) {
                       colUpsert.setObject(index++, upsertValue, sqlType);
                   } else {
                       colUpsert.setNull(index++, sqlType);
                   }
               }
               
               logger.error("the row key index in upsert events is {} ", autoGenerateKey);
               //add primary key value
               if(autoGenerateKey) {
                   sqlType = columnMetadata[offset].getSqlType();
                   String generatedRowValue = this.keyGenerator.generate();
                   Object rowkeyValue = PDataType.fromSqlType(sqlType).toObject(generatedRowValue);
                   colUpsert.setObject(index++, rowkeyValue ,sqlType);
               } 
               colUpsert.execute();
           }
           connection.commit();
       } finally {
           if(wasAutoCommit) {
               connection.setAutoCommit(true);
           }
       }
       
    }

    
    @Override
    public void close() {
       // NO-OP
    }

}
