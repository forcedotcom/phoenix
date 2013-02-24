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


/**
 * Represents a column definition for decimal during DDL, which has the following format
 * 
 * {DECIMAL} [(precision [, scale])]
 *
 * precision has a default value of 5, and scale has a default value of 0. According to
 * http://db.apache.org/derby/docs/10.7/ref/rrefsqlj15260.html#rrefsqlj15260
 * 
 * @author zhuang
 * @since 1.1
 */
public class DecimalColumnDef extends ColumnDef {
    private final Integer precision;
    private final Integer scale;

    public DecimalColumnDef(ColumnDefName columnDefName, String sqlTypeName, boolean isNull, Integer precision,
            Integer scale, boolean isPK) {
        super(columnDefName, sqlTypeName, isNull, null, isPK);
        this.precision = precision == null ? 5 : precision;
        this.scale = scale == null ? 0 : scale;
    }

    public Integer getScale() {
        return scale;
    }

    public Integer getPrecision() {
        return precision;
    }
}
