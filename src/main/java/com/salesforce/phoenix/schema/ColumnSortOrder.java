package com.salesforce.phoenix.schema;

public enum ColumnSortOrder {
	
	ASC, DESC;
	
	
	public static ColumnSortOrder defaultValue() {
		return ASC;
	}	
	
	public static ColumnSortOrder fromDDLStatement(String sortOrder) {
		return sortOrder == null ? defaultValue() : ColumnSortOrder.valueOf(sortOrder.toUpperCase());
	}
	
	public static ColumnSortOrder fromDbValue(int value) {
		switch (value) {
		    case 0: return ASC;
		    case 1: return DESC;
		    default: throw new IllegalArgumentException("Bad db value for ColumnSortOrder: " + value);
		}
	}
	
	public int toDbValue() {
		return this == ASC ? 0 : 1;
	}
}
