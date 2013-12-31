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
package com.salesforce.phoenix.jdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

/**
 * Interfaces to be implemented by classes that need to be "JDK7" compliant,
 * but also run in JDK6
 */
public final class Jdbc7Shim {

    public interface Statement {  // Note: do not extend "regular" statement or else eclipse 3.7 complains
        void closeOnCompletion() throws SQLException;
        boolean isCloseOnCompletion() throws SQLException;
    }

    public interface CallableStatement extends Statement {
        public <T> T getObject(int columnIndex, Class<T> type) throws SQLException;
        public <T> T getObject(String columnLabel, Class<T> type) throws SQLException;
    }

    public interface Connection {
         void setSchema(String schema) throws SQLException;
         String getSchema() throws SQLException;
         void abort(Executor executor) throws SQLException;
         void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException;
         int getNetworkTimeout() throws SQLException;
    }

    public interface ResultSet {
         public <T> T getObject(int columnIndex, Class<T> type) throws SQLException;
         public <T> T getObject(String columnLabel, Class<T> type) throws SQLException;
    }

    public interface DatabaseMetaData {
        java.sql.ResultSet getPseudoColumns(String catalog, String schemaPattern,
                             String tableNamePattern, String columnNamePattern)
            throws SQLException;
        boolean  generatedKeyAlwaysReturned() throws SQLException;
    }

    public interface Driver {
        public Logger getParentLogger() throws SQLFeatureNotSupportedException;
    }
}