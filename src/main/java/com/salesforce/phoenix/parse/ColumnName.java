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

import com.salesforce.phoenix.util.SchemaUtil;


public class ColumnName {
    private final NamedNode familyNode;
    private final NamedNode columnNode;
    
    public static ColumnName caseSensitiveColumnName(String familyName, String columnName) {
        return new ColumnName(NamedNode.caseSensitiveNamedNode(familyName), NamedNode.caseSensitiveNamedNode(columnName));
    }
    
    public static ColumnName caseSensitiveColumnName(String columnName) {
        return new ColumnName(null, NamedNode.caseSensitiveNamedNode(columnName));
    }
    
    private ColumnName(NamedNode familyNode, NamedNode columnNode) {
        this.familyNode = familyNode;
        this.columnNode = columnNode;
    }
    

    ColumnName(String familyName, String columnName) {
        this.familyNode = familyName == null ? null : new NamedNode(familyName);
        this.columnNode = new NamedNode(columnName);
    }

    ColumnName(String columnName) {
        this(null, columnName);
    }

    public String getFamilyName() {
        return familyNode == null ? null : familyNode.getName();
    }

    public String getColumnName() {
        return columnNode.getName();
    }

    public NamedNode getFamilyNode() {
        return familyNode;
    }

    public NamedNode getColumnNode() {
        return columnNode;
    }

    @Override
    public String toString() {
        return SchemaUtil.getColumnDisplayName(getFamilyName(),getColumnName());
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + columnNode.hashCode();
        result = prime * result + ((familyNode == null) ? 0 : familyNode.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        ColumnName other = (ColumnName)obj;
        if (!columnNode.equals(other.columnNode)) return false;
        if (familyNode == null) {
            if (other.familyNode != null) return false;
        } else if (!familyNode.equals(other.familyNode)) return false;
        return true;
    }
}
