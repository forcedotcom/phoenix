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

import java.io.IOException;
import java.sql.*;

import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.joda.time.DateTime;

import com.salesforce.phoenix.schema.PDataType;

public class TypeUtil {
	
	private static final Utf8StorageConverter utf8Converter = new Utf8StorageConverter();
	
	/**
	 * This method returns the most appropriate PDataType associated with 
	 * the incoming Pig type. Note for Pig DataType DATETIME, returns DATE as 
	 * inferredSqlType. 
	 * 
	 * This is later used to make a cast to targetPhoenixType accordingly. See
	 * {@link #castPigTypeToPhoenix(Object, byte, PDataType)}
	 * 
	 * @param obj
	 * @return
	 */
	public static PDataType getType(Object obj, byte type) {
		if (obj == null) {
			return null;
		}
	
		PDataType sqlType;

		switch (type) {
		case DataType.BYTEARRAY:
			sqlType = PDataType.BINARY;
			break;
		case DataType.CHARARRAY:
			sqlType = PDataType.VARCHAR;
			break;
		case DataType.DOUBLE:
		case DataType.FLOAT:
		//case DataType.BIGDECIMAL: not in Pig v 0.11.0
			sqlType = PDataType.DECIMAL;
			break;
		case DataType.INTEGER:
			sqlType = PDataType.INTEGER;
			break;
		case DataType.LONG:
		// case DataType.BIGINTEGER: not in Pig v 0.11.0
			sqlType = PDataType.LONG;
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
	 * This method encodes a value with Phoenix data type. It begins
	 * with checking whether an object is BINARY and makes a call to
	 * {@link #castBytes(Object, PDataType)} to convery bytes to
	 * targetPhoenixType
	 * 
	 * @param o
	 * @param targetPhoenixType
	 * @return
	 */
	public static Object castPigTypeToPhoenix(Object o, byte objectType, PDataType targetPhoenixType) {
		PDataType inferredPType = getType(o, objectType);
		
		if(inferredPType == null) {
			return null;
		}
		
		if(inferredPType == PDataType.BINARY && targetPhoenixType != PDataType.BINARY) {
			try {
				o = castBytes(o, targetPhoenixType);
				inferredPType = getType(o, DataType.findType(o));
			} catch (IOException e) {
				throw new RuntimeException("Error while casting bytes for object " +o);
			}
		}

		if(inferredPType == PDataType.DATE) {
			int inferredSqlType = targetPhoenixType.getSqlType();

			if(inferredSqlType == Types.DATE) {
				return new Date(((DateTime)o).getMillis());
			} 
			if(inferredSqlType == Types.TIME) {
				return new Time(((DateTime)o).getMillis());
			}
			if(inferredSqlType == Types.TIMESTAMP) {
				return new Timestamp(((DateTime)o).getMillis());
			}
		}
		
		if (targetPhoenixType == inferredPType || inferredPType.isCoercibleTo(targetPhoenixType)) {
			return inferredPType.toObject(o, targetPhoenixType);
		}
		
		throw new RuntimeException(o.getClass().getName()
				+ " cannot be coerced to "+targetPhoenixType.toString());
	}
	
	/**
	 * This method converts bytes to the target type required
	 * for Phoenix. It uses {@link Utf8StorageConverter} for
	 * the conversion.
	 * 
	 * @param o
	 * @param targetPhoenixType
	 * @return
	 * @throws IOException
	 */
    public static Object castBytes(Object o, PDataType targetPhoenixType) throws IOException {
        byte[] bytes = ((DataByteArray)o).get();
        
        switch(targetPhoenixType) {
        case CHAR:
        case VARCHAR:
            return utf8Converter.bytesToCharArray(bytes);
        case UNSIGNED_INT:
        case INTEGER:
            return utf8Converter.bytesToInteger(bytes);
        case BOOLEAN:
            return utf8Converter.bytesToBoolean(bytes);
//        case DECIMAL: not in Pig v 0.11.0, so using double for now
//            return utf8Converter.bytesToBigDecimal(bytes);
        case DECIMAL:
            return utf8Converter.bytesToDouble(bytes);
        case UNSIGNED_LONG:
        case LONG:
            return utf8Converter.bytesToLong(bytes);
        case TIME:
        case TIMESTAMP:
        case DATE:
        	return utf8Converter.bytesToDateTime(bytes);
        default:
        	return o;
        }        
    }

}
