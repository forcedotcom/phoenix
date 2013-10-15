/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.phoenix.join;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.hbase.index.util.ImmutableBytesPtr;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.schema.KeyValueSchema;
import com.salesforce.phoenix.schema.PColumn;
import com.salesforce.phoenix.schema.PTable;
import com.salesforce.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import com.salesforce.phoenix.util.SchemaUtil;

public class HashJoinInfo {
    private static final String HASH_JOIN = "HashJoin";
    
    private KeyValueSchema joinedSchema;
    private ImmutableBytesPtr[] joinIds;
    private List<Expression>[] joinExpressions;
    private JoinType[] joinTypes;
    private boolean[] earlyEvaluation;
    private KeyValueSchema[] schemas;
    private int[] fieldPositions;
    private Expression postJoinFilterExpression;
    
    public HashJoinInfo(PTable joinedTable, ImmutableBytesPtr[] joinIds, List<Expression>[] joinExpressions, JoinType[] joinTypes, boolean[] earlyEvaluation, PTable[] tables, int[] fieldPositions, Expression postJoinFilterExpression) {
    	this(buildSchema(joinedTable), joinIds, joinExpressions, joinTypes, earlyEvaluation, buildSchemas(tables), fieldPositions, postJoinFilterExpression);
    }
    
    private static KeyValueSchema[] buildSchemas(PTable[] tables) {
    	KeyValueSchema[] schemas = new KeyValueSchema[tables.length];
    	for (int i = 0; i < tables.length; i++) {
    		schemas[i] = buildSchema(tables[i]);
    	}
    	return schemas;
    }
    
    private static KeyValueSchema buildSchema(PTable table) {
    	KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        for (PColumn column : table.getColumns()) {
        	if (!SchemaUtil.isPKColumn(column)) {
        		builder.addField(column);
        	}
        }
        return builder.build();
    }
    
    private HashJoinInfo(KeyValueSchema joinedSchema, ImmutableBytesPtr[] joinIds, List<Expression>[] joinExpressions, JoinType[] joinTypes, boolean[] earlyEvaluation, KeyValueSchema[] schemas, int[] fieldPositions, Expression postJoinFilterExpression) {
    	this.joinedSchema = joinedSchema;
    	this.joinIds = joinIds;
        this.joinExpressions = joinExpressions;
        this.joinTypes = joinTypes;
        this.earlyEvaluation = earlyEvaluation;
        this.schemas = schemas;
        this.fieldPositions = fieldPositions;
        this.postJoinFilterExpression = postJoinFilterExpression;
    }
    
    public KeyValueSchema getJoinedSchema() {
    	return joinedSchema;
    }
    
    public ImmutableBytesPtr[] getJoinIds() {
        return joinIds;
    }
    
    public List<Expression>[] getJoinExpressions() {
        return joinExpressions;
    }
    
    public JoinType[] getJoinTypes() {
        return joinTypes;
    }
    
    public boolean[] earlyEvaluation() {
    	return earlyEvaluation;
    }
    
    public KeyValueSchema[] getSchemas() {
    	return schemas;
    }
    
    public int[] getFieldPositions() {
    	return fieldPositions;
    }
    
    public Expression getPostJoinFilterExpression() {
        return postJoinFilterExpression;
    }
    
    public static void serializeHashJoinIntoScan(Scan scan, HashJoinInfo joinInfo) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            joinInfo.joinedSchema.write(output);
            int count = joinInfo.joinIds.length;
            WritableUtils.writeVInt(output, count);
            for (int i = 0; i < count; i++) {
                joinInfo.joinIds[i].write(output);
                WritableUtils.writeVInt(output, joinInfo.joinExpressions[i].size());
                for (Expression expr : joinInfo.joinExpressions[i]) {
                    WritableUtils.writeVInt(output, ExpressionType.valueOf(expr).ordinal());
                    expr.write(output);
                }
                WritableUtils.writeVInt(output, joinInfo.joinTypes[i].ordinal());
                output.writeBoolean(joinInfo.earlyEvaluation[i]);
                joinInfo.schemas[i].write(output);
                WritableUtils.writeVInt(output, joinInfo.fieldPositions[i]);
            }
            if (joinInfo.postJoinFilterExpression != null) {
                WritableUtils.writeVInt(output, ExpressionType.valueOf(joinInfo.postJoinFilterExpression).ordinal());
                joinInfo.postJoinFilterExpression.write(output);
            } else {
                WritableUtils.writeVInt(output, -1);
            }
            scan.setAttribute(HASH_JOIN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    @SuppressWarnings("unchecked")
    public static HashJoinInfo deserializeHashJoinFromScan(Scan scan) {
        byte[] join = scan.getAttribute(HASH_JOIN);
        if (join == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(join);
        try {
            DataInputStream input = new DataInputStream(stream);
            KeyValueSchema joinedSchema = new KeyValueSchema();
            joinedSchema.readFields(input);
            int count = WritableUtils.readVInt(input);
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = new List[count];
            JoinType[] joinTypes = new JoinType[count];
            boolean[] earlyEvaluation = new boolean[count];
            KeyValueSchema[] schemas = new KeyValueSchema[count];
            int[] fieldPositions = new int[count];
            for (int i = 0; i < count; i++) {
                joinIds[i] = new ImmutableBytesPtr();
                joinIds[i].readFields(input);
                int nExprs = WritableUtils.readVInt(input);
                joinExpressions[i] = new ArrayList<Expression>(nExprs);
                for (int j = 0; j < nExprs; j++) {
                    int expressionOrdinal = WritableUtils.readVInt(input);
                    Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                    expression.readFields(input);
                    joinExpressions[i].add(expression);                    
                }
                int type = WritableUtils.readVInt(input);
                joinTypes[i] = JoinType.values()[type];
                earlyEvaluation[i] = input.readBoolean();
                schemas[i] = new KeyValueSchema();
                schemas[i].readFields(input);
                fieldPositions[i] = WritableUtils.readVInt(input);
            }
            Expression postJoinFilterExpression = null;
            int expressionOrdinal = WritableUtils.readVInt(input);
            if (expressionOrdinal != -1) {
                postJoinFilterExpression = ExpressionType.values()[expressionOrdinal].newInstance();
                postJoinFilterExpression.readFields(input);
            }
            return new HashJoinInfo(joinedSchema, joinIds, joinExpressions, joinTypes, earlyEvaluation, schemas, fieldPositions, postJoinFilterExpression);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
