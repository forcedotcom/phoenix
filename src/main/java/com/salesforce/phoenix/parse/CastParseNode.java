package com.salesforce.phoenix.parse;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import com.salesforce.phoenix.schema.PDataType;

public class CastParseNode extends UnaryParseNode {
	
	private final PDataType dt;
	
	CastParseNode(ParseNode expr, String dataType) {
		super(expr);
		dt = PDataType.fromSqlTypeName(dataType);
	}
	
	CastParseNode(ParseNode expr, PDataType dataType) {
		super(expr);
		dt = dataType;
	}

	@Override
    public <T> T accept(ParseNodeVisitor<T> visitor) throws SQLException {
        List<T> l = Collections.emptyList();
        if (visitor.visitEnter(this)) {
            l = acceptChildren(visitor);
        }
        return visitor.visitLeave(this, l);
    }

	public PDataType getDataType() {
		return dt;
	}

}
