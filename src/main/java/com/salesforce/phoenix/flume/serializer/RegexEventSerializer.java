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

import static com.salesforce.phoenix.flume.FlumeConstants.CONFIG_REGULAR_EXPRESSION;
import static com.salesforce.phoenix.flume.FlumeConstants.IGNORE_CASE_CONFIG;
import static com.salesforce.phoenix.flume.FlumeConstants.IGNORE_CASE_DEFAULT;
import static com.salesforce.phoenix.flume.FlumeConstants.REGEX_DEFAULT;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.salesforce.phoenix.schema.PDataType;

public class RegexEventSerializer extends BaseEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(RegexEventSerializer.class);
  
    private Pattern inputPattern;
    
    /**
     * 
     */
    @Override
    public void doConfigure(Context context) {
        final String regex    = context.getString(CONFIG_REGULAR_EXPRESSION, REGEX_DEFAULT);
        final boolean regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,IGNORE_CASE_DEFAULT);
        inputPattern = Pattern.compile(regex, Pattern.DOTALL + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
     }

     
    /**
     * 
     */
    @Override
    public void doInitialize() throws SQLException {
        // NO-OP
    }
    
   
    @Override
    public void upsertEvents(List<Event> events) throws SQLException {
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
               if(payloadBytes == null || payloadBytes.length == 0) {
                   continue;
               }
               String payload = new String(payloadBytes);
               Matcher m = inputPattern.matcher(payload.trim());
               
               if (!m.matches()) {
                 logger.debug("payload {} doesn't match the pattern {} ", payload, inputPattern.toString());  
                 continue;
               }
               if (m.groupCount() != colNames.size()) {
                 logger.debug("payload {} size doesn't match the pattern {} ", m.groupCount(), colNames.size());
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
                   Object upsertValue = PDataType.fromTypeId(sqlType).toObject(value);
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
                   Object upsertValue = PDataType.fromTypeId(sqlType).toObject(headerValue);
                   if (upsertValue != null) {
                       colUpsert.setObject(index++, upsertValue, sqlType);
                   } else {
                       colUpsert.setNull(index++, sqlType);
                   }
               }
  
               if(autoGenerateKey) {
                   sqlType = columnMetadata[offset].getSqlType();
                   String generatedRowValue = this.keyGenerator.generate();
                   Object rowkeyValue = PDataType.fromTypeId(sqlType).toObject(generatedRowValue);
                   colUpsert.setObject(index++, rowkeyValue ,sqlType);
               } 
               colUpsert.execute();
           }
           connection.commit();
       } catch(Exception ex){
           logger.error("An error {} occurred during persisting the event ",ex.getMessage());
           throw new SQLException(ex.getMessage());
       }finally {
           if(wasAutoCommit) {
               connection.setAutoCommit(true);
           }
       }
       
    }

}
