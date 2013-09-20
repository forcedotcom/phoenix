package com.salesforce.phoenix.parse;

public class CreateSequenceStatement implements BindableStatement {

	private final TableName sequenceName;
	private final LiteralParseNode startWith;
	private final LiteralParseNode incrementBy;
	private final int bindCount;

	protected CreateSequenceStatement(TableName sequenceName, LiteralParseNode startWith, LiteralParseNode incrementBy, int bindCount) {
		this.sequenceName = sequenceName;
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

	public TableName getSequenceName() {
		return sequenceName;
	}

	public LiteralParseNode getStartWith() {
		return startWith;
	}
}
