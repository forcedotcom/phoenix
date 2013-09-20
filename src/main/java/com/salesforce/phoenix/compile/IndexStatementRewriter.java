package com.salesforce.phoenix.compile;

import java.sql.SQLException;

import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeRewriter;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.schema.ColumnRef;
import com.salesforce.phoenix.util.IndexUtil;

public class IndexStatementRewriter extends ParseNodeRewriter {
    private final ColumnResolver resolver;
    
    public IndexStatementRewriter(ColumnResolver resolver) {
        this.resolver = resolver;
    }
    
    /**
     * Rewrite the select statement by translating all data table column references to
     * references to the corresponding index column.
     * @param statement the select statement
     * @return new select statement or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static SelectStatement translate(SelectStatement statement, ColumnResolver resolver) throws SQLException {
        return rewrite(statement, new IndexStatementRewriter(resolver));
    }
    

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        ColumnRef ref = resolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        node = NODE_FACTORY.column(IndexUtil.getIndexColumnName(ref.getColumn()));
        return node;
    }

}
