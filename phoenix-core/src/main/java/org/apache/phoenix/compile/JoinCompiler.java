/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import static org.apache.phoenix.schema.SaltingUtil.SALTING_COLUMN;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.join.ScanProjector;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.BetweenParseNode;
import org.apache.phoenix.parse.BindTableNode;
import org.apache.phoenix.parse.CaseParseNode;
import org.apache.phoenix.parse.CastParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.ConcreteTableNode;
import org.apache.phoenix.parse.DerivedTableNode;
import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.InListParseNode;
import org.apache.phoenix.parse.IsNullParseNode;
import org.apache.phoenix.parse.JoinTableNode;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.LikeParseNode;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.NotParseNode;
import org.apache.phoenix.parse.OrParseNode;
import org.apache.phoenix.parse.OrderByNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableNode;
import org.apache.phoenix.parse.TableNodeVisitor;
import org.apache.phoenix.parse.TraverseNoParseNodeVisitor;
import org.apache.phoenix.parse.WildcardParseNode;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;


public class JoinCompiler {
    
    public enum ColumnRefType {
        PREFILTER,
        JOINLOCAL,
        GENERAL,
    }
    
    public static class JoinSpec {
        private TableRef mainTable;
        private List<AliasedNode> select; // all basic nodes related to mainTable, no aggregation.
        private List<ParseNode> preFilters;
        private List<ParseNode> postFilters;
        private List<JoinTable> joinTables;
        private Map<ColumnRef, ColumnRefType> columnRefs;
        
        private JoinSpec(SelectStatement statement, ColumnResolver resolver) throws SQLException {
        	List<AliasedNode> selectList = statement.getSelect();
            List<TableNode> tableNodes = statement.getFrom();
            assert (tableNodes.size() > 1);
            Iterator<TableNode> iter = tableNodes.iterator();
            Iterator<TableRef> tableRefIter = resolver.getTables().iterator();
            iter.next();
            this.mainTable = tableRefIter.next();
            this.select = extractFromSelect(selectList, mainTable, resolver);
            this.joinTables = new ArrayList<JoinTable>(tableNodes.size() - 1);
            this.preFilters = new ArrayList<ParseNode>();
            this.postFilters = new ArrayList<ParseNode>();
            ColumnParseNodeVisitor generalRefVisitor = new ColumnParseNodeVisitor(resolver);
            ColumnParseNodeVisitor joinLocalRefVisitor = new ColumnParseNodeVisitor(resolver);
            ColumnParseNodeVisitor prefilterRefVisitor = new ColumnParseNodeVisitor(resolver);            
            boolean hasRightJoin = false;
            TableNode tableNode = null;
            while (iter.hasNext()) {
                tableNode = iter.next();
                if (!(tableNode instanceof JoinTableNode))
                    throw new SQLFeatureNotSupportedException("Full joins not supported.");
                JoinTableNode joinTableNode = (JoinTableNode) tableNode;
                JoinTable joinTable = new JoinTable(joinTableNode, tableRefIter.next(), selectList, resolver);
                joinTables.add(joinTable);
                for (ParseNode prefilter : joinTable.preFilters) {
                    prefilter.accept(prefilterRefVisitor);
                }
                for (ParseNode condition : joinTable.conditions) {
                    ComparisonParseNode comparisonNode = (ComparisonParseNode) condition;
                    comparisonNode.getLHS().accept(generalRefVisitor);
                    comparisonNode.getRHS().accept(joinLocalRefVisitor);
                }
                if (joinTable.getType() == JoinType.Right) {
                	hasRightJoin = true;
                }
            }
            if (statement.getWhere() != null) {
            	if (hasRightJoin) {
            		// conditions can't be pushed down to the scan filter.
            		postFilters.add(statement.getWhere());
            	} else {
            		statement.getWhere().accept(new WhereNodeVisitor(resolver));
            		for (ParseNode prefilter : preFilters) {
            		    prefilter.accept(prefilterRefVisitor);
            		}
            	}
            	for (ParseNode postfilter : postFilters) {
            		postfilter.accept(generalRefVisitor);
            	}
            }
            for (AliasedNode node : selectList) {
                node.getNode().accept(generalRefVisitor);
            }
            if (statement.getGroupBy() != null) {
                for (ParseNode node : statement.getGroupBy()) {
                    node.accept(generalRefVisitor);
                }
            }
            if (statement.getHaving() != null) {
                statement.getHaving().accept(generalRefVisitor);
            }
            if (statement.getOrderBy() != null) {
                for (OrderByNode node : statement.getOrderBy()) {
                    node.getNode().accept(generalRefVisitor);
                }
            }
            this.columnRefs = new HashMap<ColumnRef, ColumnRefType>();
            for (ColumnRef ref : generalRefVisitor.getColumnRefMap().keySet()) {
                columnRefs.put(ref, ColumnRefType.GENERAL);
            }
            for (ColumnRef ref : joinLocalRefVisitor.getColumnRefMap().keySet()) {
                if (!columnRefs.containsKey(ref))
                    columnRefs.put(ref, ColumnRefType.JOINLOCAL);
            }
            for (ColumnRef ref : prefilterRefVisitor.getColumnRefMap().keySet()) {
                if (!columnRefs.containsKey(ref))
                    columnRefs.put(ref, ColumnRefType.PREFILTER);
            }            
        }
        
        private JoinSpec(TableRef table, List<AliasedNode> select, List<ParseNode> preFilters, 
                List<ParseNode> postFilters, List<JoinTable> joinTables, Map<ColumnRef, ColumnRefType> columnRefs) {
            this.mainTable = table;
            this.select = select;
            this.preFilters = preFilters;
            this.postFilters = postFilters;
            this.joinTables = joinTables;
            this.columnRefs = columnRefs;
        }
        
        public TableRef getMainTable() {
            return mainTable;
        }
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<ParseNode> getPreFilters() {
            return preFilters;
        }
        
        public List<ParseNode> getPostFilters() {
            return postFilters;
        }
        
        public List<JoinTable> getJoinTables() {
            return joinTables;
        }
        
        public ParseNode getPreFiltersCombined() {
            if (preFilters == null || preFilters.isEmpty())
                return null;
            
            if (preFilters.size() == 1)
                return preFilters.get(0);
            
            return NODE_FACTORY.and(preFilters);
        }
        
        public Expression compilePostFilterExpression(StatementContext context) throws SQLException {
        	if (postFilters == null || postFilters.isEmpty())
        		return null;
        	
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            List<Expression> expressions = new ArrayList<Expression>(postFilters.size());
            for (ParseNode postFilter : postFilters) {
                expressionCompiler.reset();
                Expression expression = postFilter.accept(expressionCompiler);
                expressions.add(expression);
            }
            
            if (expressions.size() == 1)
            	return expressions.get(0);
            
            return new AndExpression(expressions);
        }

        public void projectColumns(Scan scan, TableRef table) {
            if (isWildCardSelect(select)) {
                scan.getFamilyMap().clear();
                return;
            }
            for (ColumnRef columnRef : columnRefs.keySet()) {
                if (columnRef.getTableRef().equals(table)
                        && !SchemaUtil.isPKColumn(columnRef.getColumn())) {
                    scan.addColumn(columnRef.getColumn().getFamilyName().getBytes(), columnRef.getColumn().getName().getBytes());
                }
            }
        }
        
        public ProjectedPTableWrapper createProjectedTable(TableRef tableRef, boolean retainPKColumns) throws SQLException {
        	List<PColumn> projectedColumns = new ArrayList<PColumn>();
        	List<Expression> sourceExpressions = new ArrayList<Expression>();
        	ListMultimap<String, String> columnNameMap = ArrayListMultimap.<String, String>create();
            PTable table = tableRef.getTable();
            boolean hasSaltingColumn = retainPKColumns && table.getBucketNum() != null;
            if (retainPKColumns) {
            	for (PColumn column : table.getPKColumns()) {
            		addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
            				column, tableRef, column.getFamilyName(), hasSaltingColumn);
            	}
            }
            if (isWildCardSelect(select)) {
            	for (PColumn column : table.getColumns()) {
            		if (!retainPKColumns || !SchemaUtil.isPKColumn(column)) {
            			addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
            					column, tableRef, PNameFactory.newName(ScanProjector.VALUE_COLUMN_FAMILY), hasSaltingColumn);
            		}
            	}
            } else {
                for (Map.Entry<ColumnRef, ColumnRefType> e : columnRefs.entrySet()) {
                    ColumnRef columnRef = e.getKey();
                    if (e.getValue() != ColumnRefType.PREFILTER 
                            && columnRef.getTableRef().equals(tableRef)
                            && (!retainPKColumns || !SchemaUtil.isPKColumn(columnRef.getColumn()))) {
                    	PColumn column = columnRef.getColumn();
            			addProjectedColumn(projectedColumns, sourceExpressions, columnNameMap,
            					column, tableRef, PNameFactory.newName(ScanProjector.VALUE_COLUMN_FAMILY), hasSaltingColumn);
                    }
                }            	
            }
            
            PTable t = PTableImpl.makePTable(PNameFactory.newName(PROJECTED_TABLE_SCHEMA), table.getName(), PTableType.JOIN, table.getIndexState(),
                        table.getTimeStamp(), table.getSequenceNumber(), table.getPKName(), retainPKColumns ? table.getBucketNum() : null,
                        projectedColumns, table.getParentTableName(), table.getIndexes(),
                        table.isImmutableRows(), Collections.<PName>emptyList(), null, null, table.isWALDisabled(), table.isMultiTenant(), table.getViewType());
            return new ProjectedPTableWrapper(t, columnNameMap, sourceExpressions);
        }
        
        private static void addProjectedColumn(List<PColumn> projectedColumns, List<Expression> sourceExpressions,
        		ListMultimap<String, String> columnNameMap, PColumn sourceColumn, TableRef sourceTable, PName familyName, boolean hasSaltingColumn) 
        throws SQLException {
            if (sourceColumn == SALTING_COLUMN)
                return;
            
        	int position = projectedColumns.size() + (hasSaltingColumn ? 1 : 0);
        	PTable table = sourceTable.getTable();
        	PName colName = sourceColumn.getName();
        	PName name = sourceTable.getTableAlias() == null ? null : PNameFactory.newName(getProjectedColumnName(null, sourceTable.getTableAlias(), colName.getString()));
        	PName fullName = getProjectedColumnName(table.getSchemaName(), table.getTableName(), colName);
        	if (name == null) {
        	    name = fullName;
        	} else {
        		columnNameMap.put(fullName.getString(), name.getString());
        	}
            columnNameMap.put(colName.getString(), name.getString());
    		PColumnImpl column = new PColumnImpl(name, familyName, sourceColumn.getDataType(), 
    				sourceColumn.getMaxLength(), sourceColumn.getScale(), sourceColumn.isNullable(), 
    				position, sourceColumn.getColumnModifier(), sourceColumn.getArraySize());
        	Expression sourceExpression = new ColumnRef(sourceTable, sourceColumn.getPosition()).newColumnExpression();
        	projectedColumns.add(column);
        	sourceExpressions.add(sourceExpression);
        }
        
        public boolean hasPostReference(TableRef table) {
            if (isWildCardSelect(select)) 
                return true;
            
            for (Map.Entry<ColumnRef, ColumnRefType> e : columnRefs.entrySet()) {
                if (e.getValue() == ColumnRefType.GENERAL && e.getKey().getTableRef().equals(table)) {
                    return true;
                }
            }
            
            return false;
        }
        
        private class WhereNodeVisitor  extends TraverseNoParseNodeVisitor<Void> {
            private ColumnResolver resolver;
            
            public WhereNodeVisitor(ColumnResolver resolver) {
                this.resolver = resolver;
            }
            
            private Void leaveBooleanNode(ParseNode node,
                    List<Void> l) throws SQLException {
                ColumnParseNodeVisitor visitor = new ColumnParseNodeVisitor(resolver);
                node.accept(visitor);
                ColumnParseNodeVisitor.ContentType type = visitor.getContentType(mainTable);
                if (type == ColumnParseNodeVisitor.ContentType.NONE 
                        || type == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                    preFilters.add(node);
                } else {
                    postFilters.add(node);
                }
                return null;
            }

            @Override
            public Void visitLeave(LikeParseNode node,
                    List<Void> l) throws SQLException {                
                return leaveBooleanNode(node, l);
            }

            @Override
            public boolean visitEnter(AndParseNode node) {
                return true;
            }
            
            @Override
            public Void visitLeave(OrParseNode node, List<Void> l)
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(ComparisonParseNode node, List<Void> l) 
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(NotParseNode node, List<Void> l)
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(InListParseNode node,
                    List<Void> l) throws SQLException {
                return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(IsNullParseNode node, List<Void> l) 
                    throws SQLException {
                return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(FunctionParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(BetweenParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CaseParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CastParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveBooleanNode(node, l);
            }			
        }
    }
    
    public static JoinSpec getSubJoinSpecWithoutPostFilters(JoinSpec join) {
        return new JoinSpec(join.mainTable, join.select, join.preFilters, new ArrayList<ParseNode>(), 
                join.joinTables.subList(0, join.joinTables.size() - 1), join.columnRefs);
    }
    
    public static class JoinTable {
        private JoinType type;
        private TableNode tableNode; // original table node
        private TableRef table;
        private List<AliasedNode> select; // all basic nodes related to this table, no aggregation.
        private List<ParseNode> preFilters;
        private List<ParseNode> conditions;
        private SelectStatement subquery;
        
        private Set<TableRef> leftTableRefs;
        
        public JoinTable(JoinTableNode node, TableRef tableRef, List<AliasedNode> select, ColumnResolver resolver) throws SQLException {
            if (!(node.getTable() instanceof ConcreteTableNode))
                throw new SQLFeatureNotSupportedException("Subqueries not supported.");
            
            this.type = node.getType();
            this.tableNode = node.getTable();
            this.table = tableRef;
            this.select = extractFromSelect(select,tableRef,resolver);
            this.preFilters = new ArrayList<ParseNode>();
            this.conditions = new ArrayList<ParseNode>();
            this.leftTableRefs = new HashSet<TableRef>();
            node.getOnNode().accept(new OnNodeVisitor(resolver));
        }
        
        public JoinType getType() {
            return type;
        }
        
        public TableNode getTableNode() {
            return tableNode;
        }
        
        public TableRef getTable() {
            return table;
        }
        
        public List<AliasedNode> getSelect() {
            return select;
        }
        
        public List<ParseNode> getPreFilters() {
            return preFilters;
        }
        
        public List<ParseNode> getJoinConditions() {
            return conditions;
        }
        
        public SelectStatement getSubquery() {
            return subquery;
        }
        
        public Set<TableRef> getLeftTableRefs() {
            return leftTableRefs;
        }
        
        public ParseNode getPreFiltersCombined() {
            if (preFilters == null || preFilters.isEmpty())
                return null;
            
            if (preFilters.size() == 1)
                return preFilters.get(0);
            
            return NODE_FACTORY.and(preFilters);
        }
        
        public SelectStatement getAsSubquery() {
            if (subquery != null)
                return subquery;
            
            List<TableNode> from = new ArrayList<TableNode>(1);
            from.add(tableNode);
            return NODE_FACTORY.select(from, null, false, select, getPreFiltersCombined(), null, null, null, null, 0, false);
        }
        
        public Pair<List<Expression>, List<Expression>> compileJoinConditions(StatementContext context, ColumnResolver leftResolver, ColumnResolver rightResolver) throws SQLException {
        	ColumnResolver resolver = context.getResolver();
            List<Pair<Expression, Expression>> compiled = new ArrayList<Pair<Expression, Expression>>(conditions.size());
        	context.setResolver(leftResolver);
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            for (ParseNode condition : conditions) {
                assert (condition instanceof EqualParseNode);
                EqualParseNode equalNode = (EqualParseNode) condition;
                expressionCompiler.reset();
                Expression left = equalNode.getLHS().accept(expressionCompiler);
                compiled.add(new Pair<Expression, Expression>(left, null));
            }
        	context.setResolver(rightResolver);
            expressionCompiler = new ExpressionCompiler(context);
            Iterator<Pair<Expression, Expression>> iter = compiled.iterator();
            for (ParseNode condition : conditions) {
                Pair<Expression, Expression> p = iter.next();
                EqualParseNode equalNode = (EqualParseNode) condition;
                expressionCompiler.reset();
                Expression right = equalNode.getRHS().accept(expressionCompiler);
                Expression left = p.getFirst();
                PDataType toType = getCommonType(left.getDataType(), right.getDataType());
                if (left.getDataType() != toType) {
                    left = CoerceExpression.create(left, toType);
                    p.setFirst(left);
                }
                if (right.getDataType() != toType) {
                    right = CoerceExpression.create(right, toType);
                }
                p.setSecond(right);
            }
            context.setResolver(resolver); // recover the resolver
            Collections.sort(compiled, new Comparator<Pair<Expression, Expression>>() {
                @Override
                public int compare(Pair<Expression, Expression> o1, Pair<Expression, Expression> o2) {
                    Expression e1 = o1.getFirst();
                    Expression e2 = o2.getFirst();
                    boolean isFixed1 = e1.getDataType().isFixedWidth();
                    boolean isFixed2 = e2.getDataType().isFixedWidth();
                    boolean isFixedNullable1 = e1.isNullable() &&isFixed1;
                    boolean isFixedNullable2 = e2.isNullable() && isFixed2;
                    if (isFixedNullable1 == isFixedNullable2) {
                        if (isFixed1 == isFixed2) {
                            return 0;
                        } else if (isFixed1) {
                            return -1;
                        } else {
                            return 1;
                        }
                    } else if (isFixedNullable1) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });
            List<Expression> lConditions = new ArrayList<Expression>(compiled.size());
            List<Expression> rConditions = new ArrayList<Expression>(compiled.size());
            for (Pair<Expression, Expression> pair : compiled) {
                lConditions.add(pair.getFirst());
                rConditions.add(pair.getSecond());
            }
            
            return new Pair<List<Expression>, List<Expression>>(lConditions, rConditions);
        }
        
        private PDataType getCommonType(PDataType lType, PDataType rType) throws SQLException {
            if (lType == rType)
                return lType;
            
            if (!lType.isComparableTo(rType))
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.CANNOT_CONVERT_TYPE)
                    .setMessage("On-clause LHS expression and RHS expression must be comparable. LHS type: " + lType + ", RHS type: " + rType)
                    .build().buildException();

            if ((lType == null || lType.isCoercibleTo(PDataType.TINYINT))
                    && (rType == null || rType.isCoercibleTo(PDataType.TINYINT))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.SMALLINT))
                    && (rType == null || rType.isCoercibleTo(PDataType.SMALLINT))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.INTEGER))
                    && (rType == null || rType.isCoercibleTo(PDataType.INTEGER))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.LONG))
                    && (rType == null || rType.isCoercibleTo(PDataType.LONG))) {
                return lType == null ? rType : lType; // to preserve UNSIGNED type
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.DECIMAL))
                    && (rType == null || rType.isCoercibleTo(PDataType.DECIMAL))) {
                return PDataType.DECIMAL;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.DATE))
                    && (rType == null || rType.isCoercibleTo(PDataType.DATE))) {
                return lType == null ? rType : lType;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.TIMESTAMP))
                    && (rType == null || rType.isCoercibleTo(PDataType.TIMESTAMP))) {
                return lType == null ? rType : lType;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.VARCHAR))
                    && (rType == null || rType.isCoercibleTo(PDataType.VARCHAR))) {
                return PDataType.VARCHAR;
            }

            if ((lType == null || lType.isCoercibleTo(PDataType.BOOLEAN))
                    && (rType == null || rType.isCoercibleTo(PDataType.BOOLEAN))) {
                return PDataType.BOOLEAN;
            }

            return PDataType.VARBINARY;
        }
        
        private class OnNodeVisitor  extends TraverseNoParseNodeVisitor<Void> {
            private ColumnResolver resolver;
            
            public OnNodeVisitor(ColumnResolver resolver) {
                this.resolver = resolver;
            }
            
            private Void leaveNonEqBooleanNode(ParseNode node,
                    List<Void> l) throws SQLException {
                ColumnParseNodeVisitor visitor = new ColumnParseNodeVisitor(resolver);
                node.accept(visitor);
                ColumnParseNodeVisitor.ContentType type = visitor.getContentType(table);
                if (type == ColumnParseNodeVisitor.ContentType.NONE 
                        || type == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                    preFilters.add(node);
                } else {
                    throwUnsupportedJoinConditionException();
                }
                return null;
            }

            @Override
            public Void visitLeave(LikeParseNode node,
                    List<Void> l) throws SQLException {                
                return leaveNonEqBooleanNode(node, l);
            }

            @Override
            public boolean visitEnter(AndParseNode node) {
                return true;
            }
            
            @Override
            public Void visitLeave(OrParseNode node, List<Void> l)
                    throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(ComparisonParseNode node, List<Void> l) 
                    throws SQLException {
                if (!(node instanceof EqualParseNode))
                    return leaveNonEqBooleanNode(node, l);
                ColumnParseNodeVisitor lhsVisitor = new ColumnParseNodeVisitor(resolver);
                ColumnParseNodeVisitor rhsVisitor = new ColumnParseNodeVisitor(resolver);
                node.getLHS().accept(lhsVisitor);
                node.getRHS().accept(rhsVisitor);
                ColumnParseNodeVisitor.ContentType lhsType = lhsVisitor.getContentType(table);
                ColumnParseNodeVisitor.ContentType rhsType = rhsVisitor.getContentType(table);
                if ((lhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY || lhsType == ColumnParseNodeVisitor.ContentType.NONE)
                		&& (rhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY || rhsType == ColumnParseNodeVisitor.ContentType.NONE)) {
                    preFilters.add(node);
                } else if (lhsType == ColumnParseNodeVisitor.ContentType.FOREIGN_ONLY 
                		&& rhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                    conditions.add(node);
                    leftTableRefs.addAll(lhsVisitor.getTableRefSet());
                } else if (rhsType == ColumnParseNodeVisitor.ContentType.FOREIGN_ONLY 
                		&& lhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                    conditions.add(NODE_FACTORY.equal(node.getRHS(), node.getLHS()));
                    leftTableRefs.addAll(rhsVisitor.getTableRefSet());
                } else {
                	throwUnsupportedJoinConditionException();
                }
                return null;
            }

            @Override
            public Void visitLeave(NotParseNode node, List<Void> l)
                    throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }

            @Override
            public Void visitLeave(InListParseNode node,
                    List<Void> l) throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(IsNullParseNode node, List<Void> l) 
                    throws SQLException {
                return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(FunctionParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(BetweenParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CaseParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }
            
            @Override
            public Void visitLeave(CastParseNode node, List<Void> l) 
            		throws SQLException {
            	return leaveNonEqBooleanNode(node, l);
            }

            /*
             * Conditions in the ON clause can only be:
             * 1) an equal test between a self table expression and a foreign 
             *    table expression.
             * 2) a boolean condition referencing to the self table only.
             * Otherwise, it can be ambiguous.
             */
            private void throwUnsupportedJoinConditionException() 
            		throws SQLFeatureNotSupportedException {
            	throw new SQLFeatureNotSupportedException("Does not support non-standard or non-equi join conditions.");
            }			
        }
    }
    
    private static class ColumnParseNodeVisitor  extends StatelessTraverseAllParseNodeVisitor {
        public enum ContentType {NONE, SELF_ONLY, FOREIGN_ONLY, COMPLEX};
        
        private ColumnResolver resolver;
        private final Set<TableRef> tableRefSet;
        private final Map<ColumnRef, ColumnParseNode> columnRefMap;
       
        public ColumnParseNodeVisitor(ColumnResolver resolver) {
            this.resolver = resolver;
            this.tableRefSet = new HashSet<TableRef>();
            this.columnRefMap = new HashMap<ColumnRef, ColumnParseNode>();
        }
        
        public void reset() {
            this.tableRefSet.clear();
            this.columnRefMap.clear();
        }
        
        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            ColumnRef columnRef = resolver.resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
            columnRefMap.put(columnRef, node);
            tableRefSet.add(columnRef.getTableRef());
            return null;                
        }
        
        public Set<TableRef> getTableRefSet() {
            return tableRefSet;
        }
        
        public Map<ColumnRef, ColumnParseNode> getColumnRefMap() {
            return columnRefMap;
        }
        
        public ContentType getContentType(TableRef selfTable) {
            if (tableRefSet.isEmpty())
                return ContentType.NONE;
            if (tableRefSet.size() > 1)
                return ContentType.COMPLEX;
            if (tableRefSet.contains(selfTable))
                return ContentType.SELF_ONLY;
            return ContentType.FOREIGN_ONLY;
        }
    }
    
    private static String PROJECTED_TABLE_SCHEMA = ".";
    // for creation of new statements
    private static ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();
    
    private static boolean isWildCardSelect(List<AliasedNode> select) {
        return (select.size() == 1 && select.get(0).getNode() == WildcardParseNode.INSTANCE);
    }
    
    private static List<AliasedNode> extractFromSelect(List<AliasedNode> select, TableRef table, ColumnResolver resolver) throws SQLException {
        List<AliasedNode> ret = new ArrayList<AliasedNode>();
        if (isWildCardSelect(select)) {
            ret.add(NODE_FACTORY.aliasedNode(null, WildcardParseNode.INSTANCE));
            return ret;
        }
        
        ColumnParseNodeVisitor visitor = new ColumnParseNodeVisitor(resolver);
        for (AliasedNode node : select) {
            node.getNode().accept(visitor);
            ColumnParseNodeVisitor.ContentType type = visitor.getContentType(table);
            if (type == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                ret.add(node);
            } else if (type == ColumnParseNodeVisitor.ContentType.COMPLEX) {
                for (Map.Entry<ColumnRef, ColumnParseNode> entry : visitor.getColumnRefMap().entrySet()) {
                    if (entry.getKey().getTableRef().equals(table)) {
                        ret.add(NODE_FACTORY.aliasedNode(null, entry.getValue()));
                    }
                }
            }
            visitor.reset();
        }
        return ret;
    }
    
    public static JoinSpec getJoinSpec(StatementContext context, SelectStatement statement) throws SQLException {
        return new JoinSpec(statement, context.getResolver());
    }
    
    public static SelectStatement optimize(StatementContext context, SelectStatement select, PhoenixStatement statement) throws SQLException {
        ColumnResolver resolver = context.getResolver();
        JoinSpec join = new JoinSpec(select, resolver);
        Map<TableRef, TableRef> replacement = new HashMap<TableRef, TableRef>();
        List<TableNode> from = select.getFrom();
        List<TableNode> newFrom = Lists.newArrayListWithExpectedSize(from.size());

        class TableNodeRewriter implements TableNodeVisitor {
            private TableRef table;
            private TableNode replaced;
            
            TableNodeRewriter(TableRef table) {
                this.table = table;
            }
            
            public TableNode getReplacedTableNode() {
                return replaced;
            }

            @Override
            public void visit(BindTableNode boundTableNode) throws SQLException {
                replaced = NODE_FACTORY.bindTable(boundTableNode.getAlias(), getReplacedTableName());
            }

            @Override
            public void visit(JoinTableNode joinNode) throws SQLException {
                joinNode.getTable().accept(this);
                replaced = NODE_FACTORY.join(joinNode.getType(), joinNode.getOnNode(), replaced);
            }

            @Override
            public void visit(NamedTableNode namedTableNode)
                    throws SQLException {
                replaced = NODE_FACTORY.namedTable(namedTableNode.getAlias(), getReplacedTableName(), namedTableNode.getDynamicColumns());
            }

            @Override
            public void visit(DerivedTableNode subselectNode)
                    throws SQLException {
                throw new SQLFeatureNotSupportedException();
            }
            
            private TableName getReplacedTableName() {
                String schemaName = table.getTable().getSchemaName().getString();
                schemaName = schemaName.length() == 0 ? null : '"' + schemaName + '"';
                String tableName = '"' + table.getTable().getTableName().getString() + '"';
                return NODE_FACTORY.table(schemaName, tableName);
            }
        };
        
        // get optimized plans for join tables
        for (int i = 1; i < from.size(); i++) {
            TableNode jNode = from.get(i);
            assert (jNode instanceof JoinTableNode);
            TableNode tNode = ((JoinTableNode) jNode).getTable();
            for (JoinTable jTable : join.getJoinTables()) {
                if (jTable.getTableNode() != tNode)
                    continue;
                TableRef table = jTable.getTable();
                SelectStatement stmt = getSubqueryForOptimizedPlan(select, table, join.columnRefs, jTable.getPreFiltersCombined());
                QueryPlan plan = context.getConnection().getQueryServices().getOptimizer().optimize(stmt, statement);
                if (!plan.getTableRef().equals(table)) {
                    TableNodeRewriter rewriter = new TableNodeRewriter(plan.getTableRef());
                    jNode.accept(rewriter);
                    newFrom.add(rewriter.getReplacedTableNode());
                    replacement.put(table, plan.getTableRef());
                } else {
                    newFrom.add(jNode);
                }
            }
        }
        // get optimized plan for main table
        TableRef table = join.getMainTable();
        SelectStatement stmt = getSubqueryForOptimizedPlan(select, table, join.columnRefs, join.getPreFiltersCombined());
        QueryPlan plan = context.getConnection().getQueryServices().getOptimizer().optimize(stmt, statement);
        if (!plan.getTableRef().equals(table)) {
            TableNodeRewriter rewriter = new TableNodeRewriter(plan.getTableRef());
            from.get(0).accept(rewriter);
            newFrom.add(0, rewriter.getReplacedTableNode());
            replacement.put(table, plan.getTableRef());            
        } else {
            newFrom.add(0, from.get(0));
        }
        
        if (replacement.isEmpty()) 
            return select;
        
        return IndexStatementRewriter.translate(NODE_FACTORY.select(select, newFrom), resolver, replacement);        
    }
    
    private static SelectStatement getSubqueryForOptimizedPlan(SelectStatement select, TableRef table, Map<ColumnRef, ColumnRefType> columnRefs, ParseNode where) {
        TableName tName = NODE_FACTORY.table(table.getTable().getSchemaName().getString(), table.getTable().getTableName().getString());
        List<AliasedNode> selectList = new ArrayList<AliasedNode>();
        if (isWildCardSelect(select.getSelect())) {
            selectList.add(NODE_FACTORY.aliasedNode(null, WildcardParseNode.INSTANCE));
        } else {
            for (ColumnRef colRef : columnRefs.keySet()) {
                if (colRef.getTableRef().equals(table)) {
                    selectList.add(NODE_FACTORY.aliasedNode(null, NODE_FACTORY.column(tName, '"' + colRef.getColumn().getName().getString() + '"', null)));
                }
            }
        }
        List<? extends TableNode> from = Collections.singletonList(NODE_FACTORY.namedTable(table.getTableAlias(), tName));

        return NODE_FACTORY.select(from, select.getHint(), false, selectList, where, null, null, null, null, 0, false);
    }
    
    /**
     * Returns a boolean vector indicating whether the evaluation of join expressions
     * can be evaluated at an early stage if the input JoinSpec can be taken as a
     * star join. Otherwise returns null.  
     * @param join the JoinSpec
     * @return a boolean vector for a star join; or null for non star join.
     */
    public static boolean[] getStarJoinVector(JoinSpec join) {
        assert(!join.getJoinTables().isEmpty());
        
        int count = join.getJoinTables().size();
        boolean[] vector = new boolean[count];
        for (int i = 0; i < count; i++) {
        	JoinTable joinTable = join.getJoinTables().get(i);
            if (joinTable.getType() != JoinType.Left 
                    && joinTable.getType() != JoinType.Inner)
                return null;
            vector[i] = true;
            Iterator<TableRef> iter = joinTable.getLeftTableRefs().iterator();
            while (vector[i] == true && iter.hasNext()) {
            	TableRef tableRef = iter.next();
                if (!tableRef.equals(join.getMainTable())) {
                    vector[i] = false;
                }
            }
        }
        
        return vector;
    }
    
    public static SelectStatement getSubqueryWithoutJoin(SelectStatement statement, JoinSpec join) {
        return NODE_FACTORY.select(statement.getFrom().subList(0, 1), statement.getHint(), statement.isDistinct(), statement.getSelect(), join.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    // Get the last join table select statement with fixed-up select and where nodes.
    // Currently does NOT support last join table as a subquery.
    public static SelectStatement getSubqueryForLastJoinTable(SelectStatement statement, JoinSpec join) throws SQLException {
        List<JoinTable> joinTables = join.getJoinTables();
        int count = joinTables.size();
        assert (count > 0);
        JoinTable lastJoinTable = joinTables.get(count - 1);
        if (lastJoinTable.getSubquery() != null) {
            throw new SQLFeatureNotSupportedException("Subqueries not supported.");
        }
        List<TableNode> from = new ArrayList<TableNode>(1);
        from.add(lastJoinTable.getTableNode());
        
        return NODE_FACTORY.select(from, statement.getHint(), statement.isDistinct(), statement.getSelect(), lastJoinTable.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    // Get subquery with fixed select and where nodes
    public static SelectStatement getSubQueryWithoutLastJoin(SelectStatement statement, JoinSpec join) {
        List<TableNode> from = statement.getFrom();
        assert(from.size() > 1);
        List<JoinTable> joinTables = join.getJoinTables();
        int count = joinTables.size();
        assert (count > 0);
        List<AliasedNode> select = new ArrayList<AliasedNode>();
        select.addAll(join.getSelect());
        for (int i = 0; i < count - 1; i++) {
            select.addAll(joinTables.get(i).getSelect());
        }
        
        return NODE_FACTORY.select(from.subList(0, from.size() - 1), statement.getHint(), false, select, join.getPreFiltersCombined(), null, null, null, null, statement.getBindCount(), false);
    }
    
    public static PTableWrapper mergeProjectedTables(PTableWrapper lWrapper, PTableWrapper rWrapper, boolean innerJoin) throws SQLException {
    	PTable left = lWrapper.getTable();
    	PTable right = rWrapper.getTable();
    	List<PColumn> merged = new ArrayList<PColumn>();
    	merged.addAll(left.getColumns());
    	int position = merged.size();
    	for (PColumn c : right.getColumns()) {
    		if (!SchemaUtil.isPKColumn(c)) {
    			PColumnImpl column = new PColumnImpl(c.getName(), 
    					PNameFactory.newName(ScanProjector.VALUE_COLUMN_FAMILY), c.getDataType(), 
    					c.getMaxLength(), c.getScale(), innerJoin ? c.isNullable() : true, position++, 
    					c.getColumnModifier(), c.getArraySize());
    			merged.add(column);
    		}
    	}
        if (left.getBucketNum() != null) {
            merged.remove(0);
        }
        PTable t = PTableImpl.makePTable(left.getSchemaName(), PNameFactory.newName(SchemaUtil.getTableName(left.getName().getString(), right.getName().getString())),
                left.getType(), left.getIndexState(), left.getTimeStamp(), left.getSequenceNumber(), left.getPKName(), left.getBucketNum(), merged, left.getParentTableName(),
                left.getIndexes(), left.isImmutableRows(), Collections.<PName>emptyList(), null, null, PTable.DEFAULT_DISABLE_WAL, left.isMultiTenant(), left.getViewType());

        ListMultimap<String, String> mergedMap = ArrayListMultimap.<String, String>create();
        mergedMap.putAll(lWrapper.getColumnNameMap());
        mergedMap.putAll(rWrapper.getColumnNameMap());
        
        return new PTableWrapper(t, mergedMap);
    }
    
    public static ScanProjector getScanProjector(ProjectedPTableWrapper table) {
    	return new ScanProjector(table);
    }
    
    public static class PTableWrapper {
    	protected PTable table;
    	protected ListMultimap<String, String> columnNameMap;
    	
    	protected PTableWrapper(PTable table, ListMultimap<String, String> columnNameMap) {
    		this.table = table;
    		this.columnNameMap = columnNameMap;
    	}
    	
    	public PTable getTable() {
    		return table;
    	}
    	
    	public ListMultimap<String, String> getColumnNameMap() {
    		return columnNameMap;
    	}

    	public List<String> getMappedColumnName(String name) {
    		return columnNameMap.get(name);
    	}
    }
    
    public static class ProjectedPTableWrapper extends PTableWrapper {
    	private List<Expression> sourceExpressions;
    	
    	protected ProjectedPTableWrapper(PTable table, ListMultimap<String, String> columnNameMap, List<Expression> sourceExpressions) {
    		super(table, columnNameMap);
    		this.sourceExpressions = sourceExpressions;
    	}
    	
    	public Expression getSourceExpression(PColumn column) {
    		return sourceExpressions.get(column.getPosition() - (table.getBucketNum() == null ? 0 : 1));
    	}
    }
    
    public static ColumnResolver getColumnResolver(PTableWrapper table) {
    	return new JoinedTableColumnResolver(table);
    }
    
    public static class JoinedTableColumnResolver implements ColumnResolver {
    	private PTableWrapper table;
    	private List<TableRef> tableRefs;
    	
    	private JoinedTableColumnResolver(PTableWrapper table) {
    		this.table = table;
    		TableRef tableRef = new TableRef(null, table.getTable(), 0, false);
    		this.tableRefs = ImmutableList.of(tableRef);
    	}

		@Override
		public List<TableRef> getTables() {
			return tableRefs;
		}
		
		public PTableWrapper getPTableWrapper() {
			return table;
		}

		@Override
		public ColumnRef resolveColumn(String schemaName, String tableName,
				String colName) throws SQLException {
			String name = getProjectedColumnName(schemaName, tableName, colName);
			TableRef tableRef = tableRefs.get(0);
			try {
				PColumn column = tableRef.getTable().getColumn(name);
				return new ColumnRef(tableRef, column.getPosition());
			} catch (ColumnNotFoundException e) {
				List<String> names = table.getMappedColumnName(name);
				if (names.size() == 1) {
					PColumn column = tableRef.getTable().getColumn(names.get(0));
					return new ColumnRef(tableRef, column.getPosition());					
				}
				
				if (names.size() > 1) {
					throw new AmbiguousColumnException(name);
				}
				
				throw e;
			}
		}
    }
    
    private static String getProjectedColumnName(String schemaName, String tableName,
			String colName) {
    	return SchemaUtil.getColumnName(SchemaUtil.getTableName(schemaName, tableName), colName);
    }
    
    private static PName getProjectedColumnName(PName schemaName, PName tableName,
    		PName colName) {
    	String name = getProjectedColumnName(schemaName.getString(), tableName.getString(), colName.getString());
    	return PNameFactory.newName(name);
    }
}
