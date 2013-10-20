package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import com.salesforce.phoenix.parse.ColumnDef;
import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.FamilyWildcardParseNode;
import com.salesforce.phoenix.parse.NamedTableNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeFactory;
import com.salesforce.phoenix.parse.ParseNodeRewriter;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.parse.TableNode;
import com.salesforce.phoenix.parse.WildcardParseNode;
import com.salesforce.phoenix.schema.ColumnRef;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.IndexUtil;

public class IndexStatementRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private final ColumnResolver dataResolver;
    
    public IndexStatementRewriter(ColumnResolver indexResolver,  ColumnResolver dataResolver) {
        super(indexResolver);
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
    protected List<TableNode> normalizeTableNodes(SelectStatement statement) {
        ColumnResolver resolver = this.getResolver();
        TableRef tableRef = resolver.getTables().get(0);
        List<TableNode> normTableNodes = Collections.<TableNode>singletonList(
            NamedTableNode.create(
                tableRef.getTableAlias(), 
                TableName.create(
                    tableRef.getTable().getSchemaName().getString(), 
                    tableRef.getTable().getTableName().getString()),
                Collections.<ColumnDef>emptyList()));
        return normTableNodes;
    }
    
    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        ColumnRef dataColRef = dataResolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        String indexColName = IndexUtil.getIndexColumnName(dataColRef.getColumn());
        ParseNode indexColNode = new ColumnParseNode(null, indexColName, node.toString());
        // Don't provide a schema or table name, as the column name for an index column will always be unique
        ColumnRef indexColRef = getResolver().resolveColumn(null, null, indexColName);
        
        PColumn indexCol = dataColRef.getColumn();
        PColumn dataCol = indexColRef.getColumn();
        // Coerce index column reference back to same type as data column so that
        // expression behave exactly the same. No need to invert, as this will be done
        // automatically as needed.
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
