package com.salesforce.phoenix.parse;

public class CreateSequenceStatement implements SQLStatement {

	private final TableName tableName;
	private final LiteralParseNode startWith;
	private final LiteralParseNode incrementBy;
	private final int bindCount;

	protected CreateSequenceStatement(TableName sequenceName, LiteralParseNode startWith, LiteralParseNode incrementBy, int bindCount) {
		this.tableName = sequenceName;
		this.startWith = startWith;
		this.incrementBy = incrementBy;
		this.bindCount = bindCount;
	}

	@Override
	public int getBindCount() {
		return this.bindCount;
	}
	
	public LiteralParseNode getIncrementBy() {
		return incrementBy;
	}

	public TableName getTableName() {
		return tableName;
	}

	public LiteralParseNode getStartWith() {
		return startWith;
	}
}
