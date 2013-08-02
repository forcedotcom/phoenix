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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;

import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.ExpressionType;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;

public class HashJoinInfo {
    private static final String HASH_JOIN = "HashJoin";
    
    private ImmutableBytesWritable[] joinIds;
    private List<Expression>[] joinExpressions;
    private JoinType[] joinTypes;
    
    private HashJoinInfo(ImmutableBytesWritable[] joinIds, List<Expression>[] joinExpressions, JoinType[] joinTypes) {
        this.joinIds = joinIds;
        this.joinExpressions = joinExpressions;
        this.joinTypes = joinTypes;
    }
    
    public ImmutableBytesWritable[] getJoinIds() {
        return joinIds;
    }
    
    public List<Expression>[] getJoinExpressions() {
        return joinExpressions;
    }
    
    public JoinType[] getJoinTypes() {
        return joinTypes;
    }
    
    public static void serializeHashJoinIntoScan(Scan scan, HashJoinInfo joinInfo) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
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
            int count = WritableUtils.readVInt(input);
            ImmutableBytesWritable[] joinIds = new ImmutableBytesWritable[count];
            List<Expression>[] joinExpressions = new List[count];
            JoinType[] joinTypes = new JoinType[count];
            for (int i = 0; i < count; i++) {
                joinIds[i] = new ImmutableBytesWritable();
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
            }
            return new HashJoinInfo(joinIds, joinExpressions, joinTypes);
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
