package com.salesforce.phoenix.compile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.salesforce.phoenix.jdbc.PhoenixConnection;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.query.ConnectionQueryServices;
import com.salesforce.phoenix.schema.PDataType;

public class SequenceCompiler {
	
	private SequenceCompiler() {		
	} 
	
	public static void resolveSequences(StatementContext context, List<AliasedNode> select) throws SQLException{
		List<TableName> sequences = new ArrayList<TableName>();
		for (AliasedNode aliasedNode:select){
			ParseNode parseNode = aliasedNode.getNode();
			if (parseNode instanceof NextSequenceValueParseNode){
				NextSequenceValueParseNode sequenceNode = (NextSequenceValueParseNode) parseNode;
				sequences.add(sequenceNode.getTableName());
			}
		}
		PhoenixConnection connection = context.getConnection();               
        ConnectionQueryServices service = connection.getQueryServices();
		for (TableName table: sequences){
		    final byte[] schemaBytes = PDataType.VARCHAR.toBytes(table.getSchemaName());
	        final byte[] tableBytes = PDataType.VARCHAR.toBytes(table.getTableName());
		    Long newValue = service.incrementSequence(schemaBytes, tableBytes);			
			context.setNextSequenceValue(table, newValue);	        
		}
	}
}