package com.salesforce.phoenix.parse;

import java.sql.SQLException;


public class SequenceOpParseNode extends TerminalParseNode {
    public enum Op {NEXT_VALUE, CURRENT_VALUE};
	private final TableName tableName;
	private final Op op;

	public SequenceOpParseNode(TableName tableName, Op op) {
		this.tableName = tableName;
		this.op = op;
	}

	@Override
	public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
		return visitor.visit(this);
	}

	public TableName getTableName() {
		return tableName;
	}

	@Override
	public boolean isConstant() {
		return true;
	}

    public Op getOp() {
        return op;
    }
}