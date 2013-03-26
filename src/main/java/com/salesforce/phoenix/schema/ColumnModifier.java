package com.salesforce.phoenix.schema;

public enum ColumnModifier { // bitvector?
	
	SORT_DESC() {
        @Override
		public byte[] apply(byte[] bytes, int offset, int length) {
			byte[] invertedBytes = new byte[bytes.length];
			for (int i = 0; i < bytes.length; i++) {
				if (i >= offset && i < (length + offset)) {
					invertedBytes[i] = (byte)(bytes[i] ^ 0xFF);
				} else {
					invertedBytes[i] = bytes[i];
				}
			}
			return invertedBytes;
		}
	};
	
	public static ColumnModifier fromDDLStatement(String modifier) {
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
	
	public static ColumnModifier fromDbValue(int value) {
		switch (value) {
		    case 1: return SORT_DESC;
		    default: return null;
		}
	}
	
	public static int toDbValue(ColumnModifier columnModifier) {
		if (columnModifier == null) {
			return Integer.MIN_VALUE;
		}
		switch (columnModifier) {
		    case SORT_DESC: return 1;
		    default: return Integer.MIN_VALUE;
		}
	}
	
	public static byte[] apply(ColumnModifier columnModifier, byte[] bytes, int offset, int length) {
		if (columnModifier == null) {
			return bytes;
		}
		return columnModifier.apply(bytes, offset, length);
	}
	
	public abstract byte[] apply(byte[] bytes, int offset, int length);
}