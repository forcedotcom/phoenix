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

package com.salesforce.phoenix.pig.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;

import com.salesforce.phoenix.pig.TypeUtil;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.util.ColumnInfo;

/**
 * A {@link Writable} representing a Phoenix record. This class
 * does a type mapping and sets the value accordingly in the 
 * {@link PreparedStatement}
 * 
 * @author pkommireddi
 *
 */
public class PhoenixRecord implements Writable {
	
	private final List<Object> values;
	private final ResourceFieldSchema[] fieldSchemas;
	
	public PhoenixRecord(ResourceFieldSchema[] fieldSchemas) {
		this.values = new ArrayList<Object>();
		this.fieldSchemas = fieldSchemas;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {		
	}

	@Override
	public void write(DataOutput out) throws IOException {		
	}
	
	public void write(PreparedStatement statement, List<ColumnInfo> columnMetadataList) throws SQLException {
		for (int i = 0; i < columnMetadataList.size(); i++) {
			Object o = values.get(i);
			
			byte type = (fieldSchemas == null) ? DataType.findType(o) : fieldSchemas[i].getType();
			Object upsertValue = convertTypeSpecificValue(o, type, columnMetadataList.get(i).getSqlType());

			if (upsertValue != null) {
				statement.setObject(i + 1, upsertValue, columnMetadataList.get(i).getSqlType());
			} else {
				statement.setNull(i + 1, columnMetadataList.get(i).getSqlType());
			}
		}
		
		statement.execute();
	}
	
	public void add(Object value) {
		values.add(value);
	}

	private Object convertTypeSpecificValue(Object o, byte type, Integer sqlType) {
		PDataType pDataType = PDataType.fromTypeId(sqlType);

		return TypeUtil.castPigTypeToPhoenix(o, type, pDataType);
	}
}
