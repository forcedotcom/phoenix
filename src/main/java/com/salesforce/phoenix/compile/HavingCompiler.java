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
import java.util.*;

import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.exception.SQLExceptionCode;
import com.salesforce.phoenix.exception.SQLExceptionInfo;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.parse.*;
import com.salesforce.phoenix.schema.ColumnRef;


public class HavingCompiler {

    private HavingCompiler() {
    }

    public static Expression compile(StatementContext context, SelectStatement statement, GroupBy groupBy) throws SQLException {
        ParseNode having = statement.getHaving();
        if (having == null) {
            return null;
        }
        ExpressionCompiler expressionBuilder = new ExpressionCompiler(context, groupBy);
        Expression expression = having.accept(expressionBuilder);
        if (LiteralExpression.FALSE_EXPRESSION == expression) {
            context.setScanRanges(ScanRanges.NOTHING);
            return null;
        } else if (LiteralExpression.TRUE_EXPRESSION == expression) {
            return null;
        }
        if (!expressionBuilder.isAggregate()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.ONLY_AGGREGATE_IN_HAVING_CLAUSE).build().buildException();
        }
        return expression;
    }

    public static SelectStatement rewrite(StatementContext context, SelectStatement statement, GroupBy groupBy) throws SQLException {
        ParseNode having = statement.getHaving();
        if (having == null) {
            return statement;
        }
        HavingClauseVisitor visitor = new HavingClauseVisitor(context, groupBy);
        having.accept(visitor);
        statement = SelectStatementRewriter.moveFromHavingToWhereClause(statement, visitor.getMoveToWhereClauseExpressions());
        return statement;
    }

    /**
     * 
     * Visitor that figures out if an expression can be moved from the HAVING clause to
     * the WHERE clause, since it's more optimal to pre-filter instead of post-filter.
     * 
     * The visitor traverses through AND expressions only and into comparison expresssions.
     * If a comparison expression uses a GROUP BY column and does not use any aggregate
     * functions, then it's moved. For example, these HAVING expressions would be moved:
     * 
     * select count(1) from atable group by a_string having a_string = 'foo'
     * select count(1) from atable group by a_date having round(a_date,'hour') > ?
     * select count(1) from atable group by a_date,a_string having a_date > ? or a_string = 'a'
     * select count(1) from atable group by a_string,b_string having a_string = 'a' and b_string = 'b'
     * 
     * while these would not be moved:
     * 
     * select count(1) from atable having min(a_integer) < 5
     * select count(1) from atable group by a_string having count(a_string) >= 1
     * select count(1) from atable group by a_date,a_string having a_date > ? or min(a_string) = 'a'
     * select count(1) from atable group by a_date having round(min(a_date),'hour') < ?
     *
     * @author jtaylor
     * @since 0.1
     */
    private static class HavingClauseVisitor extends TraverseNoParseNodeVisitor<Void> {
        private ParseNode topNode = null;
        private boolean hasNoAggregateFunctions = true;
        private Boolean hasOnlyAggregateColumns;
        private final StatementContext context;
        private final GroupBy groupBy;
        private final Set<ParseNode> moveToWhereClause = new LinkedHashSet<ParseNode>();
        
        HavingClauseVisitor(StatementContext context, GroupBy groupBy) {
            this.context = context;
            this.groupBy = groupBy;
        }
        
        public Set<ParseNode> getMoveToWhereClauseExpressions() {
            return moveToWhereClause;
        }
        
        @Override
        public boolean visitEnter(AndParseNode node) throws SQLException {
            return true;
        }
        
        @Override
        public boolean visitEnter(OrParseNode node) throws SQLException {
            enterBooleanNode(node);
            return true;
        }
        
        @Override
        public boolean visitEnter(ComparisonParseNode node) throws SQLException {
            enterBooleanNode(node);
            return true;
        }
        
        @Override
        public boolean visitEnter(IsNullParseNode node) throws SQLException {
            enterBooleanNode(node);
            return true;
        }
        
        private void enterBooleanNode(ParseNode node) {
            if (topNode == null) {
                topNode = node;
            }
        }
        
        private void leaveBooleanNode(ParseNode node) {
            if (topNode == node) {
                if ( hasNoAggregateFunctions && !Boolean.FALSE.equals(hasOnlyAggregateColumns)) {
                    moveToWhereClause.add(node);
                }
                hasNoAggregateFunctions = true;
                hasOnlyAggregateColumns = null;
                topNode = null;
            }
        }

        @Override
        public Void visitLeave(OrParseNode node, List<Void> l) throws SQLException {
            leaveBooleanNode(node);
            return null;
        }

        @Override
        public Void visitLeave(ComparisonParseNode node, List<Void> l) throws SQLException {
            leaveBooleanNode(node);
            return null;
        }

        @Override
        public Void visitLeave(IsNullParseNode node, List<Void> l) throws SQLException {
            leaveBooleanNode(node);
            return null;
        }

        @Override
        public boolean visitEnter(FunctionParseNode node) throws SQLException {
            boolean isAggregate = node.isAggregate();
            this.hasNoAggregateFunctions = this.hasNoAggregateFunctions && !isAggregate;
            return !isAggregate;
        }

        @Override
        public boolean visitEnter(CaseParseNode node) throws SQLException {
            return true;
        }

        @Override
        public Void visit(ColumnParseNode node) throws SQLException {
            ColumnRef ref = context.getResolver().resolveColumn(node.getSchemaName(), node.getTableName(), node.getName());
            boolean isAggregateColumn = groupBy.getExpressions().indexOf(ref.newColumnExpression()) >= 0;
            if (hasOnlyAggregateColumns == null) {
                hasOnlyAggregateColumns = isAggregateColumn;
            } else {
                hasOnlyAggregateColumns &= isAggregateColumn;
            }
            
            return null;
        }

        @Override
        public boolean visitEnter(SubtractParseNode node) throws SQLException {
            return true;
        }

        @Override
        public boolean visitEnter(AddParseNode node) throws SQLException {
            return true;
        }

        @Override
        public boolean visitEnter(MultiplyParseNode node) throws SQLException {
            return true;
        }

        @Override
        public boolean visitEnter(DivideParseNode node) throws SQLException {
            return true;
        }

        @Override
        public boolean visitEnter(BetweenParseNode node) throws SQLException {
            return true;
        }
    }
}
