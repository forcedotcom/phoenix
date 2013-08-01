package com.salesforce.phoenix.parse;

import java.sql.SQLException;

public class SubqueryJoinTableNode extends JoinTableNode {
    
    private final SelectStatement select;

    SubqueryJoinTableNode(String alias, JoinType type, ParseNode on,
            SelectStatement select) {
        super(alias, type, on);
        this.select = select;
    }
    
    public SelectStatement getSelectNode() {
        return select;
    }

    @Override
    public void accept(TableNodeVisitor visitor) throws SQLException {
        visitor.visit(this);
    }

}
