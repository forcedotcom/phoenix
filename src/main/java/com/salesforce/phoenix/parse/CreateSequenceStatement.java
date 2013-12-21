package com.salesforce.phoenix.parse;


public class CreateSequenceStatement implements BindableStatement {

	private final TableName sequenceName;
	private final ParseNode startWith;
	private final ParseNode incrementBy;
	private final int bindCount;

	protected CreateSequenceStatement(TableName sequenceName, ParseNode startsWith, ParseNode incrementBy, int bindCount) {
		this.sequenceName = sequenceName;
		this.startWith = startsWith == null ? LiteralParseNode.ZERO : startsWith;
		this.incrementBy = incrementBy == null ? LiteralParseNode.ONE : incrementBy;
		this.bindCount = bindCount;
	}

	@Override
	public int getBindCount() {
		return this.bindCount;
	}
	
	public ParseNode getIncrementBy() {
		return incrementBy;
	}

	public TableName getSequenceName() {
		return sequenceName;
	}

	public ParseNode getStartWith() {
		return startWith;
	}
}