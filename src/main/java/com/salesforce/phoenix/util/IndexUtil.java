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

import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PDataType;

public class IndexUtil {

    private IndexUtil() {
    }

    // Since we cannot have nullable fixed length in a row key
    // we need to translate to variable length.
    public static PDataType getIndexColumnDataType(PColumn dataColumn) throws SQLException {
        if (!dataColumn.isNullable() || !dataColumn.getDataType().isFixedWidth()) {
            return dataColumn.getDataType();
        }
        // for INT, BIGINT
        if (dataColumn.getDataType().isCoercibleTo(PDataType.DECIMAL)) {
            return PDataType.DECIMAL;
        }
        // for CHAR
        if (dataColumn.getDataType().isCoercibleTo(PDataType.VARCHAR)) {
            return PDataType.VARCHAR;
        }
        // Should not happen;
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_INDEX_COLUMN_ON_TYPE).setColumnName(dataColumn.getName().getString())
            .setMessage("Type="+dataColumn.getDataType()).build().buildException();
    }
    
    public static String getIndexColumnName(String dataColumnFamilyName, String dataColumnName) {
        return dataColumnFamilyName == null ? dataColumnName : dataColumnFamilyName + ":" + dataColumnName;
    }
    
    public static String getIndexColumnName(PColumn dataColumn) {
        String dataColumnFamilyName = SchemaUtil.isPKColumn(dataColumn) ? null : dataColumn.getFamilyName().getString();
        return getIndexColumnName(dataColumnFamilyName, dataColumn.getName().getString());
    }

}
