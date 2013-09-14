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
package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.salesforce.phoenix.expression.AndExpression;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.join.ScanProjector;
import com.salesforce.phoenix.join.ScanProjector.ProjectionType;
import com.salesforce.phoenix.parse.AliasedNode;
import com.salesforce.phoenix.parse.AndParseNode;
import com.salesforce.phoenix.parse.CaseParseNode;
import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.ComparisonParseNode;
import com.salesforce.phoenix.parse.ConcreteTableNode;
import com.salesforce.phoenix.parse.EqualParseNode;
import com.salesforce.phoenix.parse.InListParseNode;
import com.salesforce.phoenix.parse.IsNullParseNode;
import com.salesforce.phoenix.parse.JoinTableNode;
import com.salesforce.phoenix.parse.LikeParseNode;
import com.salesforce.phoenix.parse.NotParseNode;
import com.salesforce.phoenix.parse.OrParseNode;
import com.salesforce.phoenix.parse.OrderByNode;
import com.salesforce.phoenix.parse.ParseNode;
import com.salesforce.phoenix.parse.ParseNodeFactory;
import com.salesforce.phoenix.parse.SelectStatement;
import com.salesforce.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import com.salesforce.phoenix.parse.TableNode;
import com.salesforce.phoenix.parse.TraverseNoParseNodeVisitor;
import com.salesforce.phoenix.parse.WildcardParseNode;
import com.salesforce.phoenix.parse.JoinTableNode.JoinType;
import com.salesforce.phoenix.schema.ColumnRef;
import com.salesforce.phoenix.schema.TableRef;
import com.salesforce.phoenix.util.SchemaUtil;


public class JoinCompiler {
    
    public enum StarJoinType {
        BASIC,
        EXTENDED,
        NONE,
    }
    
    public static class JoinSpec {
        private TableRef mainTable;
        private List<AliasedNode> select; // all basic nodes related to mainTable, no aggregation.
        private List<ParseNode> preFilters;
        private List<ParseNode> postFilters;
        private List<JoinTable> joinTables;
        private Set<ColumnRef> columnRefs;
        
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
            ColumnParseNodeVisitor visitor = new ColumnParseNodeVisitor(resolver);
            TableNode tableNode = null;
            for (; iter.hasNext(); tableNode = iter.next()) {
                if (!(tableNode instanceof JoinTableNode))
                    throw new SQLFeatureNotSupportedException("Full joins not supported.");
                JoinTableNode joinTableNode = (JoinTableNode) tableNode;
                JoinTable joinTable = new JoinTable(joinTableNode, tableRefIter.next(), selectList, resolver);
                joinTables.add(joinTable);
                joinTableNode.getOnNode().accept(visitor);
            }
            statement.getWhere().accept(new WhereNodeVisitor(resolver));
            for (AliasedNode node : selectList) {
                node.getNode().accept(visitor);
            }
            statement.getWhere().accept(visitor);
            for (ParseNode node : statement.getGroupBy()) {
                node.accept(visitor);
            }
            statement.getHaving().accept(visitor);
            for (OrderByNode node : statement.getOrderBy()) {
                node.getNode().accept(visitor);
            }
            this.columnRefs = visitor.getColumnRefMap().keySet();
        }
        
        private JoinSpec(TableRef table, List<AliasedNode> select, List<ParseNode> preFilters, 
                List<ParseNode> postFilters, List<JoinTable> joinTables, Set<ColumnRef> columnRefs) {
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
        
        public ScanProjector getScanProjector() {
            byte[] tableName = null;
            if (mainTable.getTableAlias() != null) {
                tableName = Bytes.toBytes(mainTable.getTableAlias());
            } else {
                tableName = mainTable.getTableName();
            }
            return new ScanProjector(ProjectionType.TABLE, tableName, null, null);
        }
        
        public List<Expression> compilePostFilterExpressions(StatementContext context) throws SQLException {
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            context.getResolver().setDisambiguateWithTable(true);
            List<Expression> ret = new ArrayList<Expression>(postFilters.size());
            for (ParseNode postFilter : postFilters) {
                expressionCompiler.reset();
                Expression expression = postFilter.accept(expressionCompiler);
                ret.add(expression);
            }
            return ret;
        }
        
        public void projectColumns(Scan scan, TableRef table) {
            if (isWildCardSelect(select)) {
                scan.getFamilyMap().clear();
                return;
            }
            for (ColumnRef columnRef : columnRefs) {
                if (columnRef.getTableRef().equals(table)
                        && !SchemaUtil.isPKColumn(columnRef.getColumn())) {
                    scan.addColumn(columnRef.getColumn().getFamilyName().getBytes(), columnRef.getColumn().getName().getBytes());
                }
            }
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
            public Void visitLeave(AndParseNode node, List<Void> l)
                    throws SQLException {
                for (ParseNode child : node.getChildren()) {
                    if (child instanceof CaseParseNode) {
                        leaveBooleanNode(child, null);
                    }
                }
                return null;
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
        }
    }
    
    public static JoinSpec getSubJoinSpec(JoinSpec join) {
        return new JoinSpec(join.mainTable, join.select, join.preFilters, join.postFilters, 
                join.joinTables.subList(0, join.joinTables.size() - 2), join.columnRefs);
    }
    
    public static class JoinTable {
        private JoinType type;
        private List<ParseNode> conditions;
        private TableNode tableNode; // original table node
        private TableRef table;
        private List<AliasedNode> select; // all basic nodes related to this table, no aggregation.
        private List<ParseNode> preFilters;
        private List<ParseNode> postFilters;
        private SelectStatement subquery;
        
        private Set<TableRef> leftTableRefs;
        
        public JoinTable(JoinTableNode node, TableRef tableRef, List<AliasedNode> select, ColumnResolver resolver) throws SQLException {
            if (!(node.getTable() instanceof ConcreteTableNode))
                throw new SQLFeatureNotSupportedException();
            
            this.type = node.getType();
            this.conditions = new ArrayList<ParseNode>();
            this.tableNode = node.getTable();
            this.table = tableRef;
            this.select = extractFromSelect(select,tableRef,resolver);
            this.preFilters = new ArrayList<ParseNode>();
            this.postFilters = new ArrayList<ParseNode>();
            this.leftTableRefs = new HashSet<TableRef>();
            node.getOnNode().accept(new OnNodeVisitor(resolver));
        }
        
        public JoinType getType() {
            return type;
        }
        
        public List<ParseNode> getJoinConditions() {
            return conditions;
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
        
        public List<ParseNode> getPostFilters() {
            return postFilters;
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
        
        public ScanProjector getScanProjector() {
            return new ScanProjector(ProjectionType.TABLE, ScanProjector.getPrefixForTable(table), null, null);
        }
        
        public List<Expression> compilePostFilterExpressions(StatementContext context) throws SQLException {
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            context.getResolver().setDisambiguateWithTable(true);
            List<Expression> ret = new ArrayList<Expression>(postFilters.size());
            for (ParseNode postFilter : postFilters) {
                expressionCompiler.reset();
                Expression expression = postFilter.accept(expressionCompiler);
                ret.add(expression);
            }
            return ret;
        }
        
        public List<Expression> compileLeftTableConditions(StatementContext context) throws SQLException {
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            context.getResolver().setDisambiguateWithTable(true);
            List<Expression> ret = new ArrayList<Expression>(conditions.size());
            for (ParseNode condition : conditions) {
                assert (condition instanceof EqualParseNode);
                EqualParseNode equalNode = (EqualParseNode) condition;
                expressionCompiler.reset();
                Expression expression = equalNode.getLHS().accept(expressionCompiler);
                ret.add(expression);
            }
            return ret;
        }
        
        public List<Expression> compileRightTableConditions(StatementContext context) throws SQLException {
            ExpressionCompiler expressionCompiler = new ExpressionCompiler(context);
            context.getResolver().setDisambiguateWithTable(true);
            List<Expression> ret = new ArrayList<Expression>(conditions.size());
            for (ParseNode condition : conditions) {
                assert (condition instanceof EqualParseNode);
                EqualParseNode equalNode = (EqualParseNode) condition;
                expressionCompiler.reset();
                Expression expression = equalNode.getRHS().accept(expressionCompiler);
                ret.add(expression);
            }
            return ret;
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
                    postFilters.add(node);
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
            public Void visitLeave(AndParseNode node, List<Void> l)
                    throws SQLException {
                for (ParseNode child : node.getChildren()) {
                    if (child instanceof CaseParseNode) {
                        leaveNonEqBooleanNode(child, null);
                    }
                }
                return null;
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
                if (lhsType == ColumnParseNodeVisitor.ContentType.COMPLEX) {
                    postFilters.add(node);
                } else if (lhsType == ColumnParseNodeVisitor.ContentType.FOREIGN_ONLY) {
                    if (rhsType == ColumnParseNodeVisitor.ContentType.NONE
                            || rhsType == ColumnParseNodeVisitor.ContentType.SELF_ONLY) {
                        conditions.add(node);
                        leftTableRefs.addAll(lhsVisitor.getTableRefSet());
                    } else {
                        postFilters.add(node);
                    }
                } else {
                    if (rhsType == ColumnParseNodeVisitor.ContentType.COMPLEX) {
                        postFilters.add(node);
                    } else if (rhsType == ColumnParseNodeVisitor.ContentType.FOREIGN_ONLY) {
                        conditions.add(NODE_FACTORY.equal(node.getRHS(), node.getLHS()));
                        leftTableRefs.addAll(rhsVisitor.getTableRefSet());
                    } else {
                        preFilters.add(node);
                    }
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
    
    public static StarJoinType getStarJoinType(JoinSpec join) {
        assert(!join.getJoinTables().isEmpty());
        
        StarJoinType starJoinType = StarJoinType.BASIC;
        for (JoinTable joinTable : join.getJoinTables()) {
            if (joinTable.getType() != JoinType.Left 
                    && joinTable.getType() != JoinType.Inner)
                return StarJoinType.NONE;
            if (starJoinType == StarJoinType.BASIC) {
                for (TableRef tableRef : joinTable.getLeftTableRefs()) {
                    if (!tableRef.equals(join.getMainTable())) {
                        starJoinType = StarJoinType.EXTENDED;
                    }
                }
            }
        }
        
        return starJoinType;
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
        List<ParseNode> filters = new ArrayList<ParseNode>();
        select.addAll(join.getSelect());
        filters.addAll(join.getPreFilters());
        for (int i = 0; i < count - 1; i++) {
            select.addAll(joinTables.get(i).getSelect());
            filters.addAll(joinTables.get(i).getPreFilters());
            filters.addAll(joinTables.get(i).getPostFilters());
        }
        ParseNode where = null;
        if (filters.size() == 1) {
            where = filters.get(0);
        } else if (filters.size() > 1) {
            where = NODE_FACTORY.and(filters);
        }
        
        return NODE_FACTORY.select(from.subList(0, from.size() - 1), statement.getHint(), false, select, where, null, null, null, null, statement.getBindCount(), false);
    }
    
    // Get subquery with complete select and where nodes
    // Throws exception if the subquery contains joins.
    public static SelectStatement getSubQueryWithoutLastJoinAsFinalPlan(SelectStatement statement, JoinSpec join) throws SQLException {
        List<TableNode> from = statement.getFrom();
        assert(from.size() > 1);
        if (from.size() > 2)
            throw new SQLFeatureNotSupportedException("Joins followed by a left join not supported.");
        
        return NODE_FACTORY.select(from.subList(0, from.size() - 1), statement.getHint(), statement.isDistinct(), statement.getSelect(), join.getPreFiltersCombined(), statement.getGroupBy(), statement.getHaving(), statement.getOrderBy(), statement.getLimit(), statement.getBindCount(), statement.isAggregate());
    }
    
    public static Expression compilePostJoinFilterExpression(StatementContext context, JoinSpec join, JoinTable joinTable) throws SQLException {
        List<Expression> postFilters = new ArrayList<Expression>();
        if (joinTable != null) {
            postFilters.addAll(joinTable.compilePostFilterExpressions(context));
        } else {
            for (JoinTable table : join.getJoinTables()) {
                postFilters.addAll(table.compilePostFilterExpressions(context));
            }
        }
        postFilters.addAll(join.compilePostFilterExpressions(context));
        Expression postJoinFilterExpression = null;
        if (postFilters.size() == 1) {
            postJoinFilterExpression = postFilters.get(0);
        } else if (postFilters.size() > 1) {
            postJoinFilterExpression = new AndExpression(postFilters);
        }
        
        return postJoinFilterExpression;
    }
}
