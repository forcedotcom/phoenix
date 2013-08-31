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

import java.nio.ByteBuffer;
import java.sql.Types;

import org.apache.hadoop.hbase.util.ByteBufferUtils;

/**
 * The datatype for PColummns that are Arrays
 */
public class PDataTypeForArray{

	public PDataTypeForArray() {
	}

	public Integer getByteSize() {
		// TODO : When fixed ARRAY size comes in we need to change this
		return null;
	}

	public byte[] toBytes(Object object, PDataType baseType, boolean fixedWidth) {
	    int size = PDataType.fromTypeId((baseType.getSqlType() + Types.ARRAY)).estimateByteSize(object);
	    ByteBuffer buffer = ByteBuffer.allocate(size);
		return bytesFromByteBuffer((PhoenixArray)object, buffer, ((PhoenixArray)object).dimensions, fixedWidth);
	}

	public int toBytes(Object object, byte[] bytes, int offset) {
		// TODO:??
		return 0;
	}

	public boolean isCoercibleTo(PDataType targetType, Object value) {
	    return targetType.isCoercibleTo(targetType, value);
	}
	
	public boolean isCoercibleTo(PDataType targetType, PDataType expectedTargetType) {
        return targetType == expectedTargetType;
    }


    public Object toObject(String value) {
		// TODO: Do this as done in CSVLoader
		throw new IllegalArgumentException("This operation is not suppported");
	}

	public Object toObject(byte[] bytes, int offset, int length,  boolean fixedWidth, PDataType baseType, 
			ColumnModifier columnModifier) {
		return createPhoenixArray(bytes, offset, length, columnModifier,
				fixedWidth, baseType);
	}

	public Object toObject(byte[] bytes, int offset, int length, boolean fixedWidth, PDataType baseType) {
		return toObject(bytes, offset, length, fixedWidth, baseType, null);
	}

	public Object toObject(Object object) {
		// the object that is passed here is the actual array set in the params
		return object;
	}

	public Object toObject(Object object, ColumnModifier sortOrder) {
		// How to use the sortOrder ? Just reverse the elements
		return toObject(object);
	}

	// Making this private
	private byte[] bytesFromByteBuffer(PhoenixArray array, ByteBuffer buffer,
			int noOfElements, boolean fixedWidthElements) {
	    if(buffer == null) return null;
		ByteBufferUtils.writeVLong(buffer, noOfElements);
		for (int i = 0; i < noOfElements; i++) {
			if (!fixedWidthElements) {
				buffer.putInt(array.estimateByteSize(i));
			}
			byte[] bytes = array.toBytes(i);
			buffer.put(bytes);
		}
		return buffer.array();
	}

	private Object createPhoenixArray(byte[] bytes, int offset, int length,
			ColumnModifier columnModifier, boolean fixedWidthElements,
			PDataType baseDataType) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
		int noOfElements = (int) ByteBufferUtils.readVLong(buffer);
		Object[] elements = (Object[]) java.lang.reflect.Array.newInstance(
				baseDataType.getJavaClass(), noOfElements);
		for (int i = 0; i < noOfElements; i++) {
			byte[] val;
			if (!fixedWidthElements) {
				int elementSize = buffer.getInt();
				val = new byte[elementSize];
			} else {
				val = new byte[baseDataType.getByteSize()];
			}
			buffer.get(val);
			elements[i] = baseDataType.toObject(val, columnModifier);
		}
		return PDataTypeForArray.instantiatePhoenixArray(baseDataType, elements);
	}
	
    public static PhoenixArray instantiatePhoenixArray(PDataType actualType, Object[] elements) {
        return PDataType.instantiatePhoenixArray(actualType, elements);
    }
	
	public int compareTo(Object lhs, Object rhs) {
		PhoenixArray lhsArr = (PhoenixArray) lhs;
		PhoenixArray rhsArr = (PhoenixArray) rhs;
		if(lhsArr.equals(rhsArr)) {
			return 0;
		}
		return 1;
	}

}
