package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.NextSequenceValueParseNode;
import com.salesforce.phoenix.parse.OrderByNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import com.salesforce.phoenix.parse.UpsertStatement;
import com.salesforce.phoenix.query.ConnectionQueryServices;

public class SequenceCompiler {

	private SequenceCompiler() {		
	} 

	public static void resolveSequencesSelect(StatementContext context, SelectStatement select) throws SQLException{
		NextSequenceParseNodeVisitor visitor = new NextSequenceParseNodeVisitor();
        UnsupportedNextSequenceParseNodeVisitor unsupportedVisitor = new UnsupportedNextSequenceParseNodeVisitor();
		
		for (AliasedNode aliasedNode : select.getSelect()){
			ParseNode parseNode = aliasedNode.getNode();
			parseNode.accept(visitor);
		}
		ParseNode where = select.getWhere();
		if (where != null) {
		    where.accept(visitor);
		}
        for (ParseNode groupBy : select.getGroupBy()) {
            groupBy.accept(unsupportedVisitor);
        }
        ParseNode having = select.getHaving();
        if (having != null) {
            having.accept(unsupportedVisitor);
        }
        for (OrderByNode orderBy : select.getOrderBy()) {
            orderBy.getNode().accept(unsupportedVisitor);
        }
        
        resolveNextSequenceValueParseNodes(context, visitor.getNextSequenceParseNodes());
	}

	public static void resolveSequencesUpsert(StatementContext context, UpsertStatement upsert) throws SQLException{
        NextSequenceParseNodeVisitor visitor = new NextSequenceParseNodeVisitor();

        for (ParseNode groupBy : upsert.getValues()) {
            groupBy.accept(visitor);
        }
        resolveNextSequenceValueParseNodes(context, visitor.getNextSequenceParseNodes());
	}

	private static void resolveNextSequenceValueParseNodes(StatementContext context, List<NextSequenceValueParseNode> nodes) throws SQLException {
        PhoenixConnection conn = context.getConnection();
        ConnectionQueryServices services = conn.getQueryServices();
        Map<NextSequenceValueParseNode, Long> sequenceValuesMap = services.incrementSequences(nodes);
        context.setResolvedSequences(sequenceValuesMap);
	}
	
	private static class NextSequenceParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
	    private final List<NextSequenceValueParseNode> nextSequenceParseNodes = Lists.newArrayList();
	    
	    public List<NextSequenceValueParseNode> getNextSequenceParseNodes() {
	        return nextSequenceParseNodes;
	    }
	    
	    @Override
	    public Void visit(NextSequenceValueParseNode node) throws SQLException {           
	        nextSequenceParseNodes.add(node);
	        return null;
	    }
	}

	private static class UnsupportedNextSequenceParseNodeVisitor extends StatelessTraverseAllParseNodeVisitor {
        @Override
        public Void visit(NextSequenceValueParseNode node) throws SQLException {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_USE_OF_NEXT_VALUE_FOR)
            .setSchemaName(node.getTableName().getSchemaName())
            .setTableName(node.getTableName().getTableName()).build().buildException();
        }
    }
}