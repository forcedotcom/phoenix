package com.salesforce.phoenix.parse;

import java.sql.SQLException;

public class NextSequenceValueParseNode extends TerminalParseNode {
	private final TableName tableName;

	public NextSequenceValueParseNode(TableName tableName) {
		this.tableName = tableName;
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
}