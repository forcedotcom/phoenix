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
package com.salesforce.phoenix.compile;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.jdbc.PhoenixParameterMetaData;
import com.salesforce.phoenix.parse.BindParseNode;
import com.salesforce.phoenix.schema.PDatum;


/**
 * 
 * Class that manages binding parameters and checking type matching. There are
 * two main usages:
 * 
 * 1) the standard query case where we have the values for the binds.
 * 2) the retrieve param metadata case where we don't have the bind values.
 * 
 * In both cases, during query compilation we figure out what type the bind variable
 * "should" be, based on how it's used in the query. For example foo < ? would expect
 * that the bind variable type matches or can be coerced to the type of foo. For (1),
 * we check that the bind value has the correct type and for (2) we set the param
 * metadata type.
 *
 * @author jtaylor
 * @since 0.1
 */
public class BindManager {
    private final List<Object> binds;
    private final PhoenixParameterMetaData bindMetaData;

    public BindManager(List<Object> binds, int bindCount) {
        this.binds = binds;
        this.bindMetaData = new PhoenixParameterMetaData(bindCount);
    }

    public ParameterMetaData getParameterMetaData() {
        return bindMetaData;
    }
    
    public Object getBindValue(BindParseNode node) throws SQLException {
        int index = node.getIndex();
        if (index < 0 || index >= binds.size()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_INDEX_OUT_OF_BOUND)
                .setMessage("binds size: " + binds.size() + "; index: " + index).build().buildException();
        }
        return binds.get(index);
    }

    public void addParamMetaData(BindParseNode bind, PDatum column) throws SQLException {
        bindMetaData.addParam(bind,column);
    }
}
