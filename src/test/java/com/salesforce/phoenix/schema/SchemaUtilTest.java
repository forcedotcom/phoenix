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
package com.salesforce.phoenix.schema;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.salesforce.phoenix.util.SchemaUtil;

public class SchemaUtilTest {

    @Test
    public void testGetTableDisplayName() {
        String tableDisplayName = SchemaUtil.getTableDisplayName("schemaName", "tableName");
        assertEquals(tableDisplayName, "schemaName.tableName");
        tableDisplayName = SchemaUtil.getTableDisplayName(null, "tableName");
        assertEquals(tableDisplayName, "tableName");
        tableDisplayName = SchemaUtil.getTableDisplayName("schemaName", null);
        assertEquals(tableDisplayName, "schemaName");
    }

    @Test
    public void testGetColumnDisplayName() {
        String columnDisplayName;
        columnDisplayName = SchemaUtil.getColumnDisplayName("schemaName", "tableName", "familyName", "columnName");
        assertEquals(columnDisplayName, "schemaName.tableName.familyName.columnName");
        columnDisplayName = SchemaUtil.getColumnDisplayName(null, "tableName", "familyName", "columnName");
        assertEquals(columnDisplayName, "tableName.familyName.columnName");
        columnDisplayName = SchemaUtil.getColumnDisplayName("schemaName", null, "familyName", "columnName");
        assertEquals(columnDisplayName, "schemaName.familyName.columnName");
        columnDisplayName = SchemaUtil.getColumnDisplayName("schemaName", "tableName", null, "columnName");
        assertEquals(columnDisplayName, "schemaName.tableName.columnName");
        columnDisplayName = SchemaUtil.getColumnDisplayName("schemaName", "tableName", "familyName", null);
        assertEquals(columnDisplayName, "schemaName.tableName.familyName");
        columnDisplayName = SchemaUtil.getColumnDisplayName(null, null, "familyName", "columnName");
        assertEquals(columnDisplayName, "familyName.columnName");
        columnDisplayName = SchemaUtil.getColumnDisplayName("schemaName", "tableName", null, null);
        assertEquals(columnDisplayName, "schemaName.tableName");
        columnDisplayName = SchemaUtil.getColumnDisplayName(null, "tableName", "familyName", null);
        assertEquals(columnDisplayName, "tableName.familyName");
        columnDisplayName = SchemaUtil.getColumnDisplayName("schemaName", null, null, "columnName");
        assertEquals(columnDisplayName, "schemaName.columnName");
        columnDisplayName = SchemaUtil.getColumnDisplayName(null, null, null, "columnName");
        assertEquals(columnDisplayName, "columnName");
        columnDisplayName = SchemaUtil.getColumnDisplayName(null, null, "familyName", null);
        assertEquals(columnDisplayName, "familyName");
        columnDisplayName = SchemaUtil.getColumnDisplayName(null, null, null, null);
        assertEquals(columnDisplayName, "");
    }
}
