package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.NextSequenceValueParseNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.query.ConnectionQueryServices;

public class SequenceCompiler {

	private SequenceCompiler() {		
	} 

	public static void resolveSequencesSelect(StatementContext context, List<AliasedNode> select) throws SQLException{
		List<NextSequenceValueParseNode> nodes = new ArrayList<NextSequenceValueParseNode>();
		for (AliasedNode aliasedNode:select){
			ParseNode parseNode = aliasedNode.getNode();
			if (parseNode instanceof NextSequenceValueParseNode){
				NextSequenceValueParseNode sequenceNode = (NextSequenceValueParseNode) parseNode;
				nodes.add(sequenceNode);				
			}
		}
		resolveNextSequenceValueParseNodes(context, nodes);
	}

	public static void resolveSequencesUpsert(StatementContext context, List<ParseNode> upsert) throws SQLException{
		List<NextSequenceValueParseNode> nodes = new ArrayList<NextSequenceValueParseNode>();
		for (ParseNode parseNode:upsert){			
			if (parseNode instanceof NextSequenceValueParseNode){
				NextSequenceValueParseNode sequenceNode = (NextSequenceValueParseNode) parseNode;
				nodes.add(sequenceNode);			
			}
		}
		resolveNextSequenceValueParseNodes(context, nodes);
	}

	private static void resolveNextSequenceValueParseNodes(StatementContext context, List<NextSequenceValueParseNode> nodes) throws SQLException {
		List<TableName> sequenceNames = new ArrayList<TableName>();
		for (NextSequenceValueParseNode n: nodes){
			sequenceNames.add(n.getTableName());
		}		
		PhoenixConnection connection = context.getConnection();               
		ConnectionQueryServices service = connection.getQueryServices();		
		if (!sequenceNames.isEmpty()) {
			Map<TableName, Long> result = service.incrementSequences(sequenceNames);
			for (TableName tableName: result.keySet()){
				context.setNextSequenceValue(tableName, result.get(tableName));
			}
		}		
	}
}