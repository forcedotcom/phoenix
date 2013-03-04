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
package com.salesforce.phoenix.parse;

import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.SchemaUtil;

/**
 * 
 * Represents a column definition during DDL
 *
 * @author jtaylor
 * @since 0.1
 */
public class ColumnDef {
    private final ColumnDefName columnDefName;
    private final PDataType dataType;
    private final boolean isNull;
    private final Integer maxLength;
    private final boolean isPK;

    ColumnDef(ColumnDefName columnDefName, String sqlTypeName, boolean isNull, Integer maxLength, boolean isPK) {
        this.columnDefName = columnDefName;
        this.dataType = PDataType.fromSqlTypeName(SchemaUtil.normalizeIdentifier(sqlTypeName));
        this.isNull = isNull;
        if (this.dataType == PDataType.CHAR) {
            if (maxLength == null) {
                throw new IllegalArgumentException(sqlTypeName + " must declare a length");
            }
        } else if (this.dataType != PDataType.VARCHAR) {// Ignore maxLength unless CHAR or VARCHAR for now
            maxLength = null;
        }
        this.maxLength = maxLength;
        this.isPK = isPK;
    }

    public ColumnDefName getColumnDefName() {
        return columnDefName;
    }

    public PDataType getDataType() {
        return dataType;
    }

    public boolean isNull() {
        return isNull;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public boolean isPK() {
        return isPK;
    }
}
