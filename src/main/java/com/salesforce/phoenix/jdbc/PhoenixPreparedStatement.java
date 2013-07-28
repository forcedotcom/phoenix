/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source and binary forms, with
 * or without modification, are permitted provided that the following conditions are met: Redistributions of source code
 * must retain the above copyright notice, this list of conditions and the following disclaimer. Redistributions in
 * binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of Salesforce.com nor the names
 * of its contributors may be used to endorse or promote products derived from this software without specific prior
 * written permission. THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import com.salesforce.phoenix.compile.QueryPlan;
import com.salesforce.phoenix.compile.StatementPlan;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.SQLCloseable;

/**
 * JDBC PreparedStatement implementation of Phoenix. Currently only the following methods (in addition to the ones
 * supported on {@link PhoenixStatement} are supported: - {@link #executeQuery()} - {@link #setInt(int, int)} -
 * {@link #setShort(int, short)} - {@link #setLong(int, long)} - {@link #setFloat(int, float)} -
 * {@link #setDouble(int, double)} - {@link #setBigDecimal(int, BigDecimal)} - {@link #setString(int, String)} -
 * {@link #setDate(int, Date)} - {@link #setDate(int, Date, Calendar)} - {@link #setTime(int, Time)} -
 * {@link #setTime(int, Time, Calendar)} - {@link #setTimestamp(int, Timestamp)} -
 * {@link #setTimestamp(int, Timestamp, Calendar)} - {@link #setNull(int, int)} - {@link #setNull(int, int, String)} -
 * {@link #setBytes(int, byte[])} - {@link #clearParameters()} - {@link #getMetaData()}
 * 
 * @author jtaylor
 * @since 0.1
 */
public class PhoenixPreparedStatement extends PhoenixStatement implements PreparedStatement, SQLCloseable {
    private final List<Object> parameters;
    private final ExecutableStatement statement;

    private final String query;

    public PhoenixPreparedStatement(PhoenixConnection connection, PhoenixStatementParser parser) throws SQLException,
            IOException {
        super(connection);
        this.statement = parser.nextStatement(new ExecutableNodeFactory());
        if (this.statement == null) { throw new EOFException(); }
        this.query = null; // TODO: add toString on SQLStatement
        this.parameters = Arrays.asList(new Object[statement.getBindCount()]);
        Collections.fill(parameters, UNBOUND_PARAMETER);
    }

    public PhoenixPreparedStatement(PhoenixConnection connection, String query) throws SQLException {
        super(connection);
        this.query = query;
        this.statement = parseStatement(query);
        this.parameters = Arrays.asList(new Object[statement.getBindCount()]);
        Collections.fill(parameters, UNBOUND_PARAMETER);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearParameters() throws SQLException {
        Collections.fill(parameters, UNBOUND_PARAMETER);
    }

    @Override
    public List<Object> getParameters() {
        return parameters;
    }

    @Override
    public boolean execute() throws SQLException {
        throwIfUnboundParameters();
        return statement.execute();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        throwIfUnboundParameters();
        return statement.executeQuery();
    }

    public QueryPlan optimizeQuery() throws SQLException {
        throwIfUnboundParameters();
        return (QueryPlan)statement.optimizePlan();
    }


    @Override
    public int executeUpdate() throws SQLException {
        throwIfUnboundParameters();
        return statement.executeUpdate();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return statement.getResultSetMetaData();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        List<Object> nullParameters = Arrays.asList(new Object[statement.getBindCount()]);
        StatementPlan plan = statement.compilePlan(nullParameters);
        return plan.getParameterMetaData();
    }

    @Override
    public String toString() {
        return query;
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        cal.setTime(x);
        parameters.set(parameterIndex - 1, new Date(cal.getTimeInMillis()));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
//        parameters.set(parameterIndex - 1, BigDecimal.valueOf(x));
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
//        parameters.set(parameterIndex - 1, BigDecimal.valueOf(x));
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        parameters.set(parameterIndex - 1, null);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        parameters.set(parameterIndex - 1, null);
    }

    @Override
    public void setObject(int parameterIndex, Object o) throws SQLException {
        parameters.set(parameterIndex - 1, o);
    }

    @Override
    public void setObject(int parameterIndex, Object o, int targetSqlType) throws SQLException {
        PDataType targetType = PDataType.fromSqlType(targetSqlType);
        PDataType sourceType = PDataType.fromLiteral(o);
        o = targetType.toObject(o, sourceType);
        parameters.set(parameterIndex - 1, o);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        cal.setTime(x);
        parameters.set(parameterIndex - 1, new Time(cal.getTimeInMillis()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        parameters.set(parameterIndex - 1, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        cal.setTime(x);
        Timestamp value = new Timestamp(cal.getTimeInMillis());
        value.setNanos(x.getNanos());
        parameters.set(parameterIndex - 1, value);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        parameters.set(parameterIndex - 1, x.toExternalForm()); // Just treat as String
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
