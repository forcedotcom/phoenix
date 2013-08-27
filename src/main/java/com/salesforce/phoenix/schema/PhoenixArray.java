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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.util.Map;;

/**
 * java.sql.Array implementation for Phoenix
 */
public class PhoenixArray implements Array, Writable {
	int baseType;
	Object[] elements;
	int dimensions;
	byte[] buffer;

	public PhoenixArray(int baseType) {
		this.baseType = baseType;
	}

	public PhoenixArray(int baseType, Object[] elements) {
		this.baseType = baseType;
		this.elements = elements;
		this.dimensions = elements.length;
	}
	

	@Override
	public void free() throws SQLException {
	}

	@Override
	public Object getArray() throws SQLException {
		if (this.baseType == PDataType.INTEGER.ordinal()
				|| this.baseType == PDataType.UNSIGNED_INT.ordinal()) {
			Object[] intArr = (Object[]) elements;
			// Add the elements into this
			return intArr;
		} else if (this.baseType == PDataType.LONG.ordinal()
				|| this.baseType == PDataType.UNSIGNED_LONG.ordinal()) {
			Object[] longArr = (Object[]) elements;
			return longArr;
		}
		return null;
	}

	@Override
	public Object getArray(Map<String, Class<?>> map) throws SQLException {
		throw new UnsupportedOperationException("Currently not supported");
	}

	@Override
	public Object getArray(long index, int count) throws SQLException {
		if(index < 1) {
			throw new IllegalArgumentException("Index cannot be less than 1");
		}
		// Get the set of elements from the given index to the specified count
		if (this.baseType == PDataType.INTEGER.ordinal()
				|| this.baseType == PDataType.UNSIGNED_INT.ordinal()) {
			Object[] intArr = (Object[]) elements;
			boundaryCheck(index, count, intArr);
			Object[] newArr = new Object[count];
			// Add checks() here.
			int i = 0;
			for(int j = (int)index; j < count ; j++) {
				newArr[i] = intArr[j];
				i++;
			}
			return newArr;
		}
		return null;
	}

	private void boundaryCheck(long index, int count, Object[] arr) {
		if ((--index) + count > arr.length) {
			throw new IllegalArgumentException("The array index is out of range of the total number of elements in the array " + arr.length);
		}
	}

	@Override
	public Object getArray(long index, int count, Map<String, Class<?>> map)
			throws SQLException {
		if(map != null && !map.isEmpty()) {
			throw new UnsupportedOperationException("Currently not supported");
		}
		return null;
	}

	@Override
	public int getBaseType() throws SQLException {
		return baseType;
	}

	@Override
	public String getBaseTypeName() throws SQLException {
		return PDataType.fromSqlType(baseType).name();
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return null;
	}

	@Override
	public ResultSet getResultSet(Map<String, Class<?>> arg0)
			throws SQLException {
		return null;
	}

	@Override
	public ResultSet getResultSet(long arg0, int arg1) throws SQLException {
		return null;
	}

	@Override
	public ResultSet getResultSet(long arg0, int arg1,
			Map<String, Class<?>> arg2) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int compareTo(Object lhs, Object rhs) {
		Object[] lhsArr = (Object[]) lhs;
		Object[] rhsArr = (Object[]) rhs;
		if (Arrays.equals(lhsArr, rhsArr)) {
			return 0;
		}
		return 1;
	}

	public byte[][] toBytes() {
		if (this.baseType == PDataType.INTEGER.ordinal()
				|| this.baseType == PDataType.UNSIGNED_INT.ordinal()) {
			Integer[] arr = (Integer[]) elements;
			byte[][] bytes = new byte[arr.length][];
			int i = 0;
			for (Integer arrElement : arr) {
				bytes[i] = Bytes.toBytes(arrElement);
				i++;
			}
			return bytes;
		}
		return null;
	}
	
	public int getDimensions() {
		return this.dimensions;
	}
	
	public byte[] getBuffer() {
		return this.buffer;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int noOfElements = in.readInt();
		// for multidimensional Array this could not be so simple??
		if (baseType == PDataType.INTEGER.ordinal()) {
			buffer = new byte[(PDataType.INTEGER.getByteSize() * noOfElements)];			
			in.readFully(buffer, 0, buffer.length);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.elements.length);
		if (baseType == PDataType.INTEGER.ordinal()) {
			for (int i = 0; i < this.elements.length; i++) {
				byte[] bytes = PDataType.INTEGER.toBytes(
						this.elements[i]);
				out.write(bytes, 0, bytes.length);
			}
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this.dimensions != ((PhoenixArray)obj).dimensions) {
			return false;
		}
		if(this.baseType != ((PhoenixArray)obj).baseType) {
			return false;
		}
		return Arrays.equals(this.elements, ((PhoenixArray)obj).elements);
	}

	@Override
	public int hashCode() {
		// TODO : Revisit
		final int prime = 31;
		int result = 1;
		result = prime * result + ((elements == null) ? 0 : elements.hashCode());
		return result;

	}

}