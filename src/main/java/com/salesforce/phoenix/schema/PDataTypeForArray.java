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
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The datatype for PColummns that are Arrays
 */
public class PDataTypeForArray{

    private Integer byteSize;
	public PDataTypeForArray() {
	}

	public Integer getByteSize() {
		// TODO : When fixed ARRAY size comes in we need to change this
		return byteSize;
	}
	
	private void setByteSize(Integer byteSize) {
	    this.byteSize = byteSize;
	}

	public byte[] toBytes(Object object, PDataType baseType) {
	    int size;
        if (byteSize == null) {
            size = PDataType.fromTypeId((baseType.getSqlType() + Types.ARRAY)).estimateByteSize(object);
        } else {
            size = byteSize;
        }
        int noOfElements = ((PhoenixArray)object).dimensions;
        ByteBuffer buffer;
        if(byteSize == null) {
            // variable
            int capacity = initOffsetArray(noOfElements);
            buffer = ByteBuffer.allocate(size + capacity + Bytes.SIZEOF_INT);
        } else {
             buffer = ByteBuffer.allocate(size);
        }
		return bytesFromByteBuffer((PhoenixArray)object, buffer, noOfElements, byteSize);
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

	public Object toObject(byte[] bytes, int offset, int length, PDataType baseType, 
			ColumnModifier columnModifier) {
		return createPhoenixArray(bytes, offset, length, columnModifier,
				byteSize, baseType);
	}

	public Object toObject(byte[] bytes, int offset, int length, PDataType baseType) {
		return toObject(bytes, offset, length, baseType, null);
	}

	public Object toObject(Object object, PDataType actualType) {
		// the object that is passed here is the actual array set in the params
	    PDataType baseType = PDataType.fromTypeId(actualType.getSqlType() - Types.ARRAY);
	    if(!baseType.isFixedWidth()) {
	        setByteSize(null);
        } else {
            setByteSize(PDataType.fromTypeId((actualType.getSqlType())).estimateByteSize(object));
        }
		return object;
	}

	public Object toObject(Object object, PDataType actualType, ColumnModifier sortOrder) {
		// How to use the sortOrder ? Just reverse the elements
		return toObject(object, actualType);
	}

	// Making this private
	/**
	 * The format of the byte buffer looks like this for variable length array elements
	 * <noofelements><Offset of the index array><elements><offset array>
	 * where <noOfelements> - vint
	 * <offset of the index array> - int
	 * <elements>  - these are the values
	 * <offset array> - offset of every element written as INT
	 * For eg : For an varchar array with abc, defgh
	 * 
	 * @param array
	 * @param buffer
	 * @param noOfElements
	 * @param byteSize
	 * @return
	 */
	private byte[] bytesFromByteBuffer(PhoenixArray array, ByteBuffer buffer,
			int noOfElements, Integer byteSize) {
        if (buffer == null) return null;
        ByteBufferUtils.writeVLong(buffer, noOfElements);
        if (byteSize == null) {
            int fillerForOffsetByteArray = buffer.position();
            buffer.position(fillerForOffsetByteArray + Bytes.SIZEOF_INT);
            ByteBuffer offsetArray = ByteBuffer.allocate(initOffsetArray(noOfElements));
            for (int i = 0; i < noOfElements; i++) {
                // Not fixed width
                offsetArray.putInt(buffer.position());
                byte[] bytes = array.toBytes(i);
                buffer.put(bytes);
            }
            int offsetArrayPosition = buffer.position();
            buffer.put(offsetArray.array());
            buffer.position(fillerForOffsetByteArray);
            buffer.putInt(offsetArrayPosition);
        } else {
            for (int i = 0; i < noOfElements; i++) {
                byte[] bytes = array.toBytes(i);
                buffer.put(bytes);
            }
        }
        return buffer.array();
	}

	private int initOffsetArray(int noOfElements) {
		// for now create an offset array equal to the noofelements
		return noOfElements * Bytes.SIZEOF_INT;
        
    }

	private Object createPhoenixArray(byte[] bytes, int offset, int length,
			ColumnModifier columnModifier, Integer byteSize,
			PDataType baseDataType) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
		int noOfElements = (int) ByteBufferUtils.readVLong(buffer);
		Object[] elements = (Object[]) java.lang.reflect.Array.newInstance(
				baseDataType.getJavaClass(), noOfElements);
		if (byteSize == null) {
			int indexOffset = buffer.getInt();
			int valArrayPostion = buffer.position();
			buffer.position(indexOffset);
			ByteBuffer indexArr = ByteBuffer
					.allocate(initOffsetArray(noOfElements));
			byte[] array = indexArr.array();
			buffer.get(array);
			int i = 0;
			int count = 0;
			int currOff = -1;
			int nextOff = -1;
			if (noOfElements > 1) {
				while (indexArr.hasRemaining()) {
					if (i < noOfElements) {
						if (currOff == -1) {
							if ((indexArr.position() + 2 * Bytes.SIZEOF_INT) <= indexArr
									.capacity()) {
								currOff = indexArr.getInt();
								nextOff = indexArr.getInt();
								i += 2;
							}
						} else {
							currOff = nextOff;
							nextOff = indexArr.getInt();
							i += 1;
						}
						int elementLength = nextOff - currOff;
						buffer.position(currOff);
						byte[] val = new byte[elementLength];
						buffer.get(val);
						elements[count++] = baseDataType.toObject(val,
								columnModifier);
					}
				}
				buffer.position(nextOff);
				byte[] val = new byte[indexOffset - nextOff];
				buffer.get(val);
				elements[count++] = baseDataType.toObject(val, columnModifier);
			} else {
				byte[] val = new byte[indexOffset - valArrayPostion];
				buffer.position(valArrayPostion);
				buffer.get(val);
				elements[count++] = baseDataType.toObject(val, columnModifier);
			}

		} else {
			for (int i = 0; i < noOfElements; i++) {
				byte[] val;
				if (baseDataType.getByteSize() == null) {
					val = new byte[length];
				} else {
					val = new byte[baseDataType.getByteSize()];
				}
				buffer.get(val);
				elements[i] = baseDataType.toObject(val, columnModifier);
			}
		}
		return PDataTypeForArray
				.instantiatePhoenixArray(baseDataType, elements);
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
