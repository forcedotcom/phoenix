package com.salesforce.phoenix.parse;

import java.sql.SQLException;

/**
 * TableNode representing a subquery used as a join table.
 * Sub-joins like join (B join C on ...) on ... is be considered a shorthand
 * for subqueries, thus will also be represented by this node.
 * 
 * @author wxue3
 *
 */
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
