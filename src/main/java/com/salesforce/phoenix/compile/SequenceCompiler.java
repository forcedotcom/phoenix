package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.List;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.NextSequenceValueParseNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.TableName;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.schema.PDataType;

public class SequenceCompiler {

	private SequenceCompiler() {		
	} 

	public static void resolveSequencesSelect(StatementContext context, List<AliasedNode> select) throws SQLException{		
		for (AliasedNode aliasedNode:select){
			ParseNode parseNode = aliasedNode.getNode();
			if (parseNode instanceof NextSequenceValueParseNode){
				NextSequenceValueParseNode sequenceNode = (NextSequenceValueParseNode) parseNode;
				resolveNextSequenceValueParseNode(context, sequenceNode);
			}
		}
	}

	public static void resolveSequencesUpsert(StatementContext context, List<ParseNode> upsert) throws SQLException{		
		for (ParseNode parseNode:upsert){			
			if (parseNode instanceof NextSequenceValueParseNode){
				NextSequenceValueParseNode sequenceNode = (NextSequenceValueParseNode) parseNode;
				resolveNextSequenceValueParseNode(context, sequenceNode);
			}
		}
	}

	private static void resolveNextSequenceValueParseNode(StatementContext context, NextSequenceValueParseNode node) throws SQLException {
		TableName table = node.getTableName();
		PhoenixConnection connection = context.getConnection();               
		ConnectionQueryServices service = connection.getQueryServices();
		final byte[] schemaBytes = PDataType.VARCHAR.toBytes(table.getSchemaName());
		final byte[] tableBytes = PDataType.VARCHAR.toBytes(table.getTableName());
		Long newValue = service.incrementSequence(schemaBytes, tableBytes);			
		context.setNextSequenceValue(table, newValue);		
	}
}