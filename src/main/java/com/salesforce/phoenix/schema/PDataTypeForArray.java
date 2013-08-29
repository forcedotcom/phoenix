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

import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

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
		return bytesFromByteBuffer((PhoenixArray) object, baseType, fixedWidth);
	}

	public int toBytes(Object object, byte[] bytes, int offset) {
		// TODO:??
		return 0;
	}

	public boolean isCoercibleTo(PDataType targetType, Object value) {
		// Should check for every element in the array?
		return true;
	}

	public Object toObject(String value) {
		// TODO: How can this be done?
		throw new IllegalDataException("This operation is not suppported");
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

	public byte[] bytesFromByteBuffer(PhoenixArray array, PDataType baseType,
			boolean fixedWidthElements) {
		int noOfElements = array.elements.length;
		int vIntSize = WritableUtils.getVIntSize(noOfElements);
		ByteBuffer buffer;
		if (fixedWidthElements) {
			buffer = ByteBuffer.allocate(vIntSize
					+ (noOfElements * baseType.getByteSize()));
		} else {
			int totalVarSize = 0;
			for (int i = 0; i < array.elements.length; i++) {
				totalVarSize += baseType.estimateByteSize(array.elements[i]);
			}
			/**
			 * For non fixed width arrays we will write No of elements - as vint
			 * (we know the no of elements upfront so we can know how much space
			 * this vint is going to occupy) Write the size of every element as
			 * Int (cannot use vint here as we cannot fix the actual size of the
			 * ByteBuffer for allocation) Followed by the actual data -
			 * totalVarSize (this has to calculated upfront). See above for loop
			 */
			buffer = ByteBuffer.allocate(vIntSize + (totalVarSize)
					+ (noOfElements * Bytes.SIZEOF_INT));
		}
		ByteBufferUtils.writeVLong(buffer, noOfElements);
		for (int i = 0; i < array.elements.length; i++) {
			if (!fixedWidthElements) {
				buffer.putInt(baseType.estimateByteSize(array.elements[i]));
			}
			byte[] bytes = baseType.toBytes(array.elements[i]);
			buffer.put(bytes);
		}
		return buffer.array();
	}

	public Object createPhoenixArray(byte[] bytes, int offset, int length,
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
		PhoenixArray array = new PhoenixArray(baseDataType, elements);
		return array;
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
