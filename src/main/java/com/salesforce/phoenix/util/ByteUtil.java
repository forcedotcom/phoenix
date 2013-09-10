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
package com.salesforce.phoenix.util;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.schema.ColumnModifier;


/**
 * 
 * Byte utilities
 *
 * @author jtaylor
 * @since 0.1
 */
public class ByteUtil {
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    
    public static final Comparator<ImmutableBytesPtr> BYTES_PTR_COMPARATOR = new Comparator<ImmutableBytesPtr>() {

        @Override
        public int compare(ImmutableBytesPtr o1, ImmutableBytesPtr o2) {
            return Bytes.compareTo(o1.get(), o1.getOffset(), o1.getLength(), o2.get(), o2.getOffset(), o2.getLength());
        }
        
    };

    /**
     * Serialize an array of byte arrays into a single byte array.  Used
     * to pass through a set of bytes arrays as an attribute of a Scan.
     * Use {@link #toByteArrays(byte[], int)} to convert the serialized
     * byte array back to the array of byte arrays.
     * @param byteArrays the array of byte arrays to serialize
     * @return the byte array
     */
    public static byte[] toBytes(byte[][] byteArrays) {
        int size = 0;
        for (byte[] b : byteArrays) {
            if (b == null) {
                size++;
            } else {
                size += b.length;
                size += WritableUtils.getVIntSize(b.length);
            }
        }
        TrustedByteArrayOutputStream bytesOut = new TrustedByteArrayOutputStream(size);
        DataOutputStream out = new DataOutputStream(bytesOut);
        try {
            for (byte[] b : byteArrays) {
                if (b == null) {
                    WritableUtils.writeVInt(out, 0);
                } else {
                    WritableUtils.writeVInt(out, b.length);
                    out.write(b);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e); // not possible
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // not possible
            }
        }
        return bytesOut.getBuffer();
    }

    /**
     * Deserialize a byte array into a set of byte arrays.  Used in
     * coprocessor to reconstruct byte arrays from attribute value
     * passed through the Scan.
     * @param b byte array containing serialized byte arrays (created by {@link #toBytes(byte[][])}).
     * @param length number of byte arrays that were serialized
     * @return array of now deserialized byte arrays
     * @throws IllegalStateException if there are more than length number of byte arrays that were serialized
     */
    public static byte[][] toByteArrays(byte[] b, int length) {
        return toByteArrays(b, 0, length);
    }

    public static byte[][] toByteArrays(byte[] b, int offset, int length) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(b, offset, b.length - offset);
        DataInputStream in = new DataInputStream(bytesIn);
        byte[][] byteArrays = new byte[length][];
        try {
            for (int i = 0; i < length; i++) {
                int bLength = WritableUtils.readVInt(in);
                if (bLength == 0) {
                    byteArrays[i] = null;
                } else {
                    byteArrays[i] = new byte[bLength];
                    int rLength = in.read(byteArrays[i], 0, bLength);
                    assert (rLength == bLength); // For find bugs
                }
            }
            if (in.read() != -1) {
                throw new IllegalStateException("Expected only " + length + " byte arrays, but found more");
            }
            return byteArrays;
        } catch (IOException e) {
            throw new RuntimeException(e); // not possible
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // not possible
            }
        }
    }

    public static byte[] serializeIntArray(int[] intArray) {
        return serializeIntArray(intArray,intArray.length);
    }

    public static byte[] serializeIntArray(int[] intArray, int encodedLength) {
        int size = WritableUtils.getVIntSize(encodedLength);
        for (int i = 0; i < intArray.length; i++) {
            size += WritableUtils.getVIntSize(intArray[i]);
        }
        int offset = 0;
        byte[] out = new byte[size];
        offset += ByteUtil.vintToBytes(out, offset, size);
        for (int i = 0; i < intArray.length; i++) {
            offset += ByteUtil.vintToBytes(out, offset, intArray[i]);
        }
        return out;
    }

    public static void serializeIntArray(DataOutput output, int[] intArray) throws IOException {
        serializeIntArray(output, intArray, intArray.length);
    }

    /**
     * Allows additional stuff to be encoded in length
     * @param output
     * @param intArray
     * @param encodedLength
     * @throws IOException
     */
    public static void serializeIntArray(DataOutput output, int[] intArray, int encodedLength) throws IOException {
        WritableUtils.writeVInt(output, encodedLength);
        for (int i = 0; i < intArray.length; i++) {
            WritableUtils.writeVInt(output, intArray[i]);
        }
    }

    /**
     * Deserialize a byte array into a int array.  
     * @param b byte array storing serialized vints
     * @return int array
     */
    public static int[] deserializeIntArray(byte[] b) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(b);
        DataInputStream in = new DataInputStream(bytesIn);
        try {
            int length = WritableUtils.readVInt(in);
            return deserializeIntArray(in, length);
        } catch (IOException e) {
            throw new RuntimeException(e); // not possible
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // not possible
            }
        }
    }

    public static int[] deserializeIntArray(DataInput in) throws IOException {
        return deserializeIntArray(in, WritableUtils.readVInt(in));
    }

    public static int[] deserializeIntArray(DataInput in, int length) throws IOException {
        int i = 0;
        int[] intArray = new int[length];
        while (i < length) {
            intArray[i++] = WritableUtils.readVInt(in);
        }
        return intArray;
    }

    /**
     * Deserialize a byte array into a int array.  
     * @param b byte array storing serialized vints
     * @param length number of serialized vints
     * @return int array
     */
    public static int[] deserializeIntArray(byte[] b, int length) {
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(b);
        DataInputStream in = new DataInputStream(bytesIn);
        try {
            return deserializeIntArray(in,length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Concatenate together one or more byte arrays
     * @param first first byte array
     * @param rest rest of byte arrays
     * @return newly allocated byte array that is a concatenation of all the byte arrays passed in
     */
    public static byte[] concat(byte[] first, byte[]... rest) {
        int totalLength = first.length;
        for (byte[] array : rest) {
            totalLength += array.length;
        }
        byte[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (byte[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static <T> T[] concat(T[] first, T[]... rest) {
        int totalLength = first.length;
        for (T[] array : rest) {
          totalLength += array.length;
        }
        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (T[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static byte[] concat(ColumnModifier columnModifier, ImmutableBytesWritable... writables) {
        int totalLength = 0;
        for (ImmutableBytesWritable writable : writables) {
            totalLength += writable.getLength();
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (ImmutableBytesWritable array : writables) {
            byte[] bytes = array.get();
            if (columnModifier != null) {
                bytes = columnModifier.apply(bytes, array.getOffset(), new byte[array.getLength()], 0, array.getLength());
            }
            System.arraycopy(bytes, array.getOffset(), result, offset, array.getLength());
            offset += array.getLength();
        }
        return result;
    }

    public static int vintFromBytes(byte[] buffer, int offset) {
        try {
            return (int)Bytes.readVLong(buffer, offset);
        } catch (IOException e) { // Impossible
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode a vint from the buffer pointed at to by ptr and
     * increment the offset of the ptr by the length of the
     * vint.
     * @param ptr a pointer to a byte array buffer
     * @return the decoded vint value as an int
     */
    public static int vintFromBytes(ImmutableBytesWritable ptr) {
        return (int) vlongFromBytes(ptr);
    }

    /**
     * Decode a vint from the buffer pointed at to by ptr and
     * increment the offset of the ptr by the length of the
     * vint.
     * @param ptr a pointer to a byte array buffer
     * @return the decoded vint value as a long
     */
    public static long vlongFromBytes(ImmutableBytesWritable ptr) {
        final byte [] buffer = ptr.get();
        final int offset = ptr.getOffset();
        byte firstByte = buffer[offset];
        int len = WritableUtils.decodeVIntSize(firstByte);
        if (len == 1) {
            ptr.set(buffer, offset+1, ptr.getLength());
            return firstByte;
        }
        long i = 0;
        for (int idx = 0; idx < len-1; idx++) {
            byte b = buffer[offset + 1 + idx];
            i = i << 8;
            i = i | (b & 0xFF);
        }
        ptr.set(buffer, offset+len, ptr.getLength());
        return (WritableUtils.isNegativeVInt(firstByte) ? ~i : i);
    }

    
    /**
     * Put long as variable length encoded number at the offset in the result byte array
     * @param vint Integer to make a vint of.
     * @param result buffer to put vint into
     * @return Vint length in bytes of vint
     */
    public static int vintToBytes(byte[] result, int offset, final long vint) {
      long i = vint;
      if (i >= -112 && i <= 127) {
        result[offset] = (byte) i;
        return 1;
      }

      int len = -112;
      if (i < 0) {
        i ^= -1L; // take one's complement'
        len = -120;
      }

      long tmp = i;
      while (tmp != 0) {
        tmp = tmp >> 8;
        len--;
      }

      result[offset++] = (byte) len;

      len = (len < -120) ? -(len + 120) : -(len + 112);

      for (int idx = len; idx != 0; idx--) {
        int shiftbits = (idx - 1) * 8;
        long mask = 0xFFL << shiftbits;
        result[offset++] = (byte)((i & mask) >> shiftbits);
      }
      return len + 1;
    }

    /**
     * Increment the key to the next key
     * @param key the key to increment
     * @return a new byte array with the next key or null
     *  if the key could not be incremented because it's
     *  already at its max value.
     */
    public static byte[] nextKey(byte[] key) {
        byte[] nextStartRow = new byte[key.length];
        System.arraycopy(key, 0, nextStartRow, 0, key.length);
        if (!nextKey(nextStartRow, nextStartRow.length)) {
            return null;
        }
        return nextStartRow;
    }

    /**
     * Increment the key in-place to the next key
     * @param key the key to increment
     * @param length the length of the key
     * @return true if the key can be incremented and
     *  false otherwise if the key is at its max
     *  value.
     */
    public static boolean nextKey(byte[] key, int length) {
        return nextKey(key, 0, length);
    }
    
    public static boolean nextKey(byte[] key, int offset, int length) {
        if (length == 0) {
            return false;
        }
        int i = offset + length - 1;
        while (key[i] == -1) {
            key[i] = 0;
            i--;
            if (i < offset) {
                return false;
            }
         }
        key[i] = (byte)(key[i] + 1);
        return true;
    }

    /**
     * Expand the key to length bytes using the fillByte to fill the
     * bytes beyond the current key length.
     */
    public static byte[] fillKey(byte[] key, int length) {
        if(key.length > length) {
            throw new IllegalStateException();
        }
        if (key.length == length) {
            return key;
        }
        byte[] newBound = new byte[length];
        System.arraycopy(key, 0, newBound, 0, key.length);
        return newBound;
    }

    /**
     * Get the size in bytes of the UTF-8 encoded CharSequence
     * @param sequence the CharSequence
     */
    public static int getSize(CharSequence sequence) {
        int count = 0;
        for (int i = 0, len = sequence.length(); i < len; i++) {
          char ch = sequence.charAt(i);
          if (ch <= 0x7F) {
            count++;
          } else if (ch <= 0x7FF) {
            count += 2;
          } else if (Character.isHighSurrogate(ch)) {
            count += 4;
            ++i;
          } else {
            count += 3;
          }
        }
        return count;
    }

    public static boolean isInclusive(CompareOp op) {
        switch (op) {
            case LESS:
            case GREATER:
                return false;
            case EQUAL:
            case NOT_EQUAL:
            case LESS_OR_EQUAL:
            case GREATER_OR_EQUAL:
                return true;
            default:
              throw new RuntimeException("Unknown Compare op " + op.name());
        }
    }
    public static boolean compare(CompareOp op, int compareResult) {
        switch (op) {
            case LESS:
              return compareResult < 0;
            case LESS_OR_EQUAL:
              return compareResult <= 0;
            case EQUAL:
              return compareResult == 0;
            case NOT_EQUAL:
              return compareResult != 0;
            case GREATER_OR_EQUAL:
              return compareResult >= 0;
            case GREATER:
              return compareResult > 0;
            default:
              throw new RuntimeException("Unknown Compare op " + op.name());
        }
    }

    /**
     * Given an ImmutableBytesWritable, returns the payload part of the argument as an byte array. 
     */
    public static byte[] copyKeyBytesIfNecessary(ImmutableBytesWritable ptr) {
        if (ptr.getOffset() == 0 && ptr.getLength() == ptr.get().length) {
            return ptr.get();
        }
        return ptr.copyBytes();
    }
}
