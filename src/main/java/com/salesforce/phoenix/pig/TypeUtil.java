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

package com.salesforce.phoenix.pig;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.pig.data.DataType;
import org.joda.time.DateTime;

import com.salesforce.phoenix.schema.PDataType;

public class TypeUtil {

	/**
	 * This method infers incoming Pig data type and returns the most
	 * appropriate PDataType associated with it. Note for Pig DataType DATETIME,
	 * this method returns DATE as inferredSqlType. This is later used to make a
	 * cast to targetPhoenixType accordingly. See
	 * {@link #castPigTypeToPhoenix(Object, PDataType)}
	 * 
	 * @param obj
	 * @return
	 */
	public static PDataType getType(Object obj) {
		if (obj == null) {
			return null;
		}

		byte type = DataType.findType(obj);
		PDataType sqlType;

		switch (type) {
		case DataType.BYTEARRAY:
			sqlType = PDataType.BINARY;
			break;
		case DataType.CHARARRAY:
			sqlType = PDataType.VARCHAR;
			break;
		case DataType.DOUBLE:
			sqlType = PDataType.DECIMAL;
			break;
		case DataType.FLOAT:
			sqlType = PDataType.DECIMAL;
			break;
		case DataType.INTEGER:
			sqlType = PDataType.INTEGER;
			break;
		case DataType.LONG:
			sqlType = PDataType.LONG;
			break;
		case DataType.BIGINTEGER:
			sqlType = PDataType.LONG;
			break;
		case DataType.BIGDECIMAL:
			sqlType = PDataType.DECIMAL;
			break;
		case DataType.BOOLEAN:
			sqlType = PDataType.BOOLEAN;
			break;
		case DataType.DATETIME:
			sqlType = PDataType.DATE;
			break;
		default:
			throw new RuntimeException("Unknown type " + obj.getClass().getName()
					+ " passed to PhoenixHBaseStorage");
		}

		return sqlType;

	}

	/**
	 * This method encodes a value with Phoenix data type.
	 * 
	 * @param o
	 * @param targetPhoenixType
	 * @return
	 */
	public static Object castPigTypeToPhoenix(Object o, PDataType targetPhoenixType) {
		PDataType inferredPType = getType(o);
		
		if(inferredPType == null) {
			return null;
		}

		if(inferredPType == PDataType.DATE) {
			int inferredSqlType = targetPhoenixType.getSqlType();
			// if sqlType is DATE
			if(inferredSqlType == 91) {
				return new Date(((DateTime)o).getMillis());
			} 
			// if sqlType is Time
			if(inferredSqlType == 92) {
				return new Time(((DateTime)o).getMillis());
			}
			// if sqlType is Timestamp
			if(inferredSqlType == 93) {
				return new Timestamp(((DateTime)o).getMillis());
			}
		}
		
		if (targetPhoenixType == inferredPType || inferredPType.isCoercibleTo(targetPhoenixType)) {
			return inferredPType.toObject(o, targetPhoenixType);
		}
		
		throw new RuntimeException(o.getClass().getName()
				+ " cannot be coerced to "+targetPhoenixType.toString());
	}
}
