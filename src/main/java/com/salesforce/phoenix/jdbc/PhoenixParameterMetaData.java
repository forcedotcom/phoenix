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

import java.sql.*;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.parse.BindParseNode;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.PDatum;
import com.salesforce.phoenix.schema.TypeMismatchException;



/**
 * 
 * Implementation of ParameterMetaData for Phoenix
 *
 * @author jtaylor
 * @since 0.1
 */
public class PhoenixParameterMetaData implements ParameterMetaData {
    public static final PhoenixParameterMetaData EMPTY_PARAMETER_META_DATA = new PhoenixParameterMetaData(0);
    private final PDatum[] params;
    
    public PhoenixParameterMetaData(int paramCount) {
        params = new PDatum[paramCount];
    }
 
    private PDatum getParam(int index) throws SQLException {
        if (index <= 0 || index > params.length) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_INDEX_OUT_OF_BOUND)
                .setMessage("The index is " + index + ". Must be between 1 and " + params.length)
                .build().buildException();
        }
        PDatum param = params[index-1];
        if (param == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_VALUE_UNBOUND)
                .setMessage("Parameter at index " + index + " is unbound").build().buildException();
        }
        return param;
    }
    @Override
    public String getParameterClassName(int index) throws SQLException {
        PDataType type = getParam(index).getDataType();
        return type == null ? null : type.getJavaClassName();
    }

    @Override
    public int getParameterCount() throws SQLException {
        return params.length;
    }

    @Override
    public int getParameterMode(int index) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }

    @Override
    public int getParameterType(int index) throws SQLException {
        return getParam(index).getDataType().getSqlType();
    }

    @Override
    public String getParameterTypeName(int index) throws SQLException {
        return getParam(index).getDataType().getSqlTypeName();
    }

    @Override
    public int getPrecision(int index) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int index) throws SQLException {
        return 0;
    }

    @Override
    public int isNullable(int index) throws SQLException {
        return getParam(index).isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isSigned(int index) throws SQLException {
        @SuppressWarnings("rawtypes")
		Class clazz = getParam(index).getDataType().getJavaClass();
        return Number.class.isInstance(clazz);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!iface.isInstance(this)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.CLASS_NOT_UNWRAPPABLE)
                .setMessage(this.getClass().getName() + " not unwrappable from " + iface.getName())
                .build().buildException();
        }
        return (T)this;
    }

    public void addParam(BindParseNode bind, PDatum datum) throws SQLException {
        PDatum bindDatum = params[bind.getIndex()];
        if (bindDatum != null && bindDatum.getDataType() != null && !datum.getDataType().isCoercibleTo(bindDatum.getDataType())) {
            throw new TypeMismatchException(datum.getDataType(), bindDatum.getDataType());
        }
        params[bind.getIndex()] = datum;
    }
}
