package com.salesforce.phoenix.schema;

public enum ColumnModifier {
	
	SORT_DESC() {
        @Override
		public void apply(byte[] src, byte[] dest, int offset, int length) {
			for (int i = 0; i < src.length; i++) {
				if (i >= offset && i < (length + offset)) {
					dest[i] = (byte)(src[i] ^ 0xFF);
				} else {
					dest[i] = src[i];
				}
			}
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
		
	public abstract void apply(byte[] src, byte[] dest, int offset, int length);
}