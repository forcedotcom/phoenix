package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.Collections;

import com.salesforce.phoenix.expression.function.InvertFunction;
import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.FamilyWildcardParseNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeFactory;
import com.salesforce.phoenix.parse.ParseNodeRewriter;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.WildcardParseNode;
import com.salesforce.phoenix.schema.ColumnRef;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.util.IndexUtil;

public class IndexStatementRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final ColumnResolver indexResolver;
    private final ColumnResolver dataResolver;
    
    public IndexStatementRewriter(ColumnResolver indexResolver,  ColumnResolver dataResolver) {
        this.indexResolver = indexResolver;
        this.dataResolver = dataResolver;
    }
    
    /**
     * Rewrite the select statement by translating all data table column references to
     * references to the corresponding index column.
     * @param statement the select statement
     * @return new select statement or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static SelectStatement translate(SelectStatement statement, ColumnResolver indexResolver,  ColumnResolver dataResolver) throws SQLException {
        return rewrite(statement, new IndexStatementRewriter(indexResolver, dataResolver));
    }

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        ColumnRef indexColumnRef = indexResolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        // Don't provide a TableName, as the column name for an index column will always be unique
        ParseNode indexColNode = new ColumnParseNode(null, IndexUtil.getIndexColumnName(indexColumnRef.getColumn()), node.toString());
        ColumnRef dataColumnRef = dataResolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        
        PColumn indexCol = indexColumnRef.getColumn();
        PColumn dataCol = dataColumnRef.getColumn();
        if (indexCol.getColumnModifier() != dataCol.getColumnModifier()) {
            indexColNode = FACTORY.function(InvertFunction.NAME, Collections.singletonList(indexColNode));
        }
        if (!indexCol.getDataType().isBytesComparableWith(dataCol.getDataType())) {
            indexColNode = FACTORY.cast(indexColNode, dataCol.getDataType());
        }
        return indexColNode;
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
