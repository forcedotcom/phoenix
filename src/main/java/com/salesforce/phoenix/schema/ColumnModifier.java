package com.salesforce.phoenix.schema;

import com.google.common.base.Preconditions;
import com.sun.istack.NotNull;
import com.sun.istack.Nullable;

public enum ColumnModifier {
	
	SORT_DESC() {
        @Override
		public byte[] apply(byte[] src, byte[] dest, int offset, int length) {
            Preconditions.checkNotNull(src);            
            if (dest == null) {
                dest = new byte[src.length];
            }
            int maxIndex = length + offset;
			for (int i = 0; i < src.length; i++) {
				if (i >= offset && i < maxIndex) {
					dest[i] = (byte)(src[i] ^ 0xFF);
				} else {
					dest[i] = src[i];
				}
			}			
			return dest;
		}
	};
	
	public static ColumnModifier fromDDLValue(String modifier) {
		if (modifier == null) {
			return null;
		} else if (modifier.equalsIgnoreCase("ASC")) {
			return null;
		} else if (modifier.equalsIgnoreCase("DESC")) {
			return SORT_DESC;
		} else {
			return null;
		}			
	}
	
	public static ColumnModifier fromSystemValue(int value) {
		switch (value) {
		    case 1: return SORT_DESC;
		    default: return null;
		}
	}
	
	public static int toSystemValue(ColumnModifier columnModifier) {
		if (columnModifier == null) {
			return Integer.MIN_VALUE;
		}
		switch (columnModifier) {
		    case SORT_DESC: return 1;
		    default: return Integer.MIN_VALUE;
		}
	}
	
	/**
	 * Copy the bytes from the src array to the dest array and apply the column modifier operation on the bytes
	 * starting at the specified offset index.  The column modifier is applied to the number of bytes matching the 
	 * specified length.  If dest is null, a new byte array is allocated.
	 * 
	 * @param src    the src byte array to copy from, cannot be null
	 * @param dest   the byte array to copy into, if it is null, a new byte array with the same lenght as src is allocated
	 * @param offset start applying the column modifier from this index
	 * @param length apply the column modifier for this many bytes 
	 * @return       dest or a new byte array if dest is null
	 */
	public abstract byte[] apply(@NotNull byte[] src, @Nullable byte[] dest, int offset, int length);
}