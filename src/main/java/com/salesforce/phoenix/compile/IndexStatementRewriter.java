package com.salesforce.phoenix.compile;

import java.sql.SQLException;

import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.FamilyWildcardParseNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeRewriter;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.WildcardParseNode;
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
        // Don't provide a TableName, as the column name for an index column will always be unique
        return new ColumnParseNode(null, IndexUtil.getIndexColumnName(ref.getColumn()), node.toString());
    }

    @Override
    public ParseNode visit(WildcardParseNode node) throws SQLException {
        return WildcardParseNode.REWRITE_INSTANCE;
    }

    @Override
    public ParseNode visit(FamilyWildcardParseNode node) throws SQLException {
        return new FamilyWildcardParseNode(node, true);
    }
    
}
