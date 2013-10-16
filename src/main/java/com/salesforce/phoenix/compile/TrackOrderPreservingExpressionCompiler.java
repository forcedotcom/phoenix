package com.salesforce.phoenix.compile;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.salesforce.phoenix.compile.GroupByCompiler.GroupBy;
import com.salesforce.phoenix.expression.Expression;
import com.salesforce.phoenix.expression.LiteralExpression;
import com.salesforce.phoenix.expression.function.FunctionExpression;
import com.salesforce.phoenix.expression.function.FunctionExpression.OrderPreserving;
import com.salesforce.phoenix.parse.CaseParseNode;
import com.salesforce.phoenix.parse.ColumnParseNode;
import com.salesforce.phoenix.parse.DivideParseNode;
import com.salesforce.phoenix.parse.MultiplyParseNode;
import com.salesforce.phoenix.parse.SubtractParseNode;
import com.salesforce.phoenix.schema.ColumnModifier;
import com.salesforce.phoenix.schema.ColumnRef;
import com.salesforce.phoenix.util.SchemaUtil;

/**
 * Visitor that builds the expressions of a GROUP BY and ORDER BY clause. While traversing
 * the parse node tree, the visitor also determines if the natural key order of the scan
 * will match the order of the expressions. For GROUP BY, if order is preserved we can use
 * an optimization during server-side aggregation to do the aggregation on-the-fly versus
 * keeping track of each distinct group. We can only do this optimization if all the rows
 * for each group will be contiguous. For ORDER BY, we can drop the ORDER BY statement if
 * the order is preserved.
 * 
 */
public class TrackOrderPreservingExpressionCompiler  extends ExpressionCompiler {
    public enum Ordering {ORDERED, UNORDERED};
    
    private final List<Entry> entries;
    private final Ordering ordering;
    private final int positionOffset;
    private OrderPreserving orderPreserving = OrderPreserving.YES;
    private ColumnRef columnRef;
    private boolean isOrderPreserving = true;
    
    TrackOrderPreservingExpressionCompiler(StatementContext context, GroupBy groupBy, int expectedEntrySize, Ordering ordering) {
        super(context, groupBy);
        positionOffset =  context.getResolver().getTables().get(0).getTable().getBucketNum() == null ? 0 : 1;
        entries = Lists.newArrayListWithExpectedSize(expectedEntrySize);
        this.ordering = ordering;
    }
    

    public boolean isOrderPreserving() {
        if (!isOrderPreserving) {
            return false;
        }
        if (ordering == Ordering.UNORDERED) {
            // Sort by position
            Collections.sort(entries, new Comparator<Entry>() {
                @Override
                public int compare(Entry o1, Entry o2) {
                    return o1.getPkPosition()-o2.getPkPosition();
                }
            });
        }
        // Determine if there are any gaps in the PK columns (in which case we don't need
        // to sort in the coprocessor because the keys will already naturally be in sorted
        // order.
        int prevPos = positionOffset - 1;
        OrderPreserving prevOrderPreserving = OrderPreserving.YES;
        for (int i = 0; i < entries.size() && isOrderPreserving; i++) {
            Entry entry = entries.get(i);
            int pos = entry.getPkPosition();
            isOrderPreserving &= (entry.getOrderPreserving() != OrderPreserving.NO) && (pos == prevPos || ((pos - 1 == prevPos) && (prevOrderPreserving == OrderPreserving.YES)));
            prevPos = pos;
            prevOrderPreserving = entries.get(i).getOrderPreserving();
        }
        return isOrderPreserving;
    }
    
    @Override
    protected Expression addFunction(FunctionExpression func) {
        // Keep the minimum value between this function and the current value,
        // so that we never increase OrderPreserving from NO or YES_IF_LAST.
        orderPreserving = OrderPreserving.values()[Math.min(orderPreserving.ordinal(), func.preservesOrder().ordinal())];
        return super.addFunction(func);
    }

    @Override
    public boolean visitEnter(CaseParseNode node) throws SQLException {
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }
    
    @Override
    public boolean visitEnter(DivideParseNode node) throws SQLException {
        // A divide expression may not preserve row order.
        // For example: GROUP BY 1/x
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }

    @Override
    public boolean visitEnter(SubtractParseNode node) throws SQLException {
        // A subtract expression may not preserve row order.
        // For example: GROUP BY 10 - x
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }

    @Override
    public boolean visitEnter(MultiplyParseNode node) throws SQLException {
        // A multiply expression may not preserve row order.
        // For example: GROUP BY -1 * x
        orderPreserving = OrderPreserving.NO;
        return super.visitEnter(node);
    }

    @Override
    public void reset() {
        super.reset();
        columnRef = null;
        orderPreserving = OrderPreserving.YES;
    }
    
    @Override
    protected ColumnRef resolveColumn(ColumnParseNode node) throws SQLException {
        ColumnRef ref = super.resolveColumn(node);
        // If we encounter any non PK column, then we can't aggregate on-the-fly
        // because the distinct groups have no correlation to the KV column value
        if (!SchemaUtil.isPKColumn(ref.getColumn())) {
            orderPreserving = OrderPreserving.NO;
        }
        
        if (columnRef == null) {
            columnRef = ref;
        } else if (!columnRef.equals(ref)) {
            // If we encounter more than one column reference in an expression,
            // we can't assume the result of the expression will be key ordered.
            // For example GROUP BY a * b
            orderPreserving = OrderPreserving.NO;
        }
        return ref;
    }

    public boolean addEntry(Expression expression) {
        if (expression instanceof LiteralExpression) {
            return false;
        }
        isOrderPreserving &= (orderPreserving != OrderPreserving.NO);
        entries.add(new Entry(expression, columnRef, orderPreserving));
        return true;
    }
    
    public boolean addEntry(Expression expression, ColumnModifier modifier) {
        // If the expression is sorted in a different order than the specified sort order
        // then the expressions are not order preserving.
        if (!Objects.equal(expression.getColumnModifier(), modifier)) {
            orderPreserving = OrderPreserving.NO;
        }
        return addEntry(expression);
    }
    
    public List<Entry> getEntries() {
        return entries;
    }

    public static class Entry {
        private final Expression expression;
        private final ColumnRef columnRef;
        private final OrderPreserving orderPreserving;
        
        private Entry(Expression expression, ColumnRef columnRef, OrderPreserving orderPreserving) {
            this.expression = expression;
            this.columnRef = columnRef;
            this.orderPreserving = orderPreserving;
        }

        public Expression getExpression() {
            return expression;
        }

        public int getPkPosition() {
            return columnRef.getPKSlotPosition();
        }

        public int getColumnPosition() {
            return columnRef.getColumnPosition();
        }

        public OrderPreserving getOrderPreserving() {
            return orderPreserving;
        }
    }
}