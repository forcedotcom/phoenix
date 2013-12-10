package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.Map;

import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.FamilyWildcardParseNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeFactory;
import com.salesforce.phoenix.parse.ParseNodeRewriter;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.parse.WildcardParseNode;
import com.salesforce.phoenix.schema.ColumnRef;
import com.salesforce.phoenix.schema.PDataType;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.IndexUtil;

public class IndexStatementRewriter extends ParseNodeRewriter {
    private static final ParseNodeFactory FACTORY = new ParseNodeFactory();
    
    private Map<TableRef, TableRef> multiTableRewriteMap;
    
    public IndexStatementRewriter(ColumnResolver dataResolver, Map<TableRef, TableRef> multiTableRewriteMap) {
        super(dataResolver);
        this.multiTableRewriteMap = multiTableRewriteMap;
    }
    
    /**
     * Rewrite the select statement by translating all data table column references to
     * references to the corresponding index column.
     * @param statement the select statement
     * @param resolver the column resolver
     * @return new select statement or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static SelectStatement translate(SelectStatement statement, ColumnResolver dataResolver) throws SQLException {
        return translate(statement, dataResolver, null);
    }
    
    /**
     * Rewrite the select statement containing multiple tables by translating all 
     * data table column references to references to the corresponding index column.
     * @param statement the select statement
     * @param resolver the column resolver
     * @param multiTableRewriteMap the data table to index table map
     * @return new select statement or the same one if nothing was rewritten.
     * @throws SQLException 
     */
    public static SelectStatement translate(SelectStatement statement, ColumnResolver dataResolver, Map<TableRef, TableRef> multiTableRewriteMap) throws SQLException {
        return rewrite(statement, new IndexStatementRewriter(dataResolver, multiTableRewriteMap));
    }

    @Override
    public ParseNode visit(ColumnParseNode node) throws SQLException {
        ColumnRef dataColRef = getResolver().resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
        TableName tName = null;
        if (multiTableRewriteMap != null) {
            TableRef tableRef = multiTableRewriteMap.get(dataColRef.getTableRef());
            if (tableRef == null)
                return node;
            
            String schemaName = tableRef.getTable().getSchemaName().getString();
            schemaName = schemaName.length() == 0 ? null : '"' + schemaName + '"';
            String tableName = '"' + tableRef.getTable().getTableName().getString() + '"';
            tName = FACTORY.table(schemaName, tableName);
        }
        String indexColName = IndexUtil.getIndexColumnName(dataColRef.getColumn());
        // Same alias as before, but use the index column name instead of the data column name
        ParseNode indexColNode = new ColumnParseNode(tName, indexColName, node.toString());
        PDataType indexColType = IndexUtil.getIndexColumnDataType(dataColRef.getColumn());
        PDataType dataColType = dataColRef.getColumn().getDataType();

        // Coerce index column reference back to same type as data column so that
        // expression behave exactly the same. No need to invert, as this will be done
        // automatically as needed. If node is used at the top level, do not convert, as
        // otherwise the wrapper gets in the way in the group by clause. For example,
        // an INTEGER column in a GROUP BY gets doubly wrapped like this:
        //     CAST CAST int_col AS INTEGER AS DECIMAL
        // This is unnecessary and problematic in the case of a null value.
        // TODO: test case for this
        if (!isTopLevel() && indexColType != dataColType) {
            indexColNode = FACTORY.cast(indexColNode, dataColType);
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
